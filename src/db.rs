use std::collections::HashMap;
use std::sync::LazyLock;

use axum::{
    Extension,
    extract::{Path, Request},
    middleware::Next,
    response::Response,
};
use surrealdb::{
    Surreal,
    engine::any::Any,
    opt::{Config, capabilities::Capabilities},
    sql::statements::CommitStatement,
};
use tokio::sync::{Mutex, OnceCell, RwLock};
use tracing::{info, instrument, warn};

use crate::{
    Result,
    account::{Account, AccountQueries},
    auth::{DashboardAuth, ReportApiKeyAuth},
    env::Env,
};
use archodex_error::{
    anyhow::{self, Context as _},
    not_found,
};

#[derive(Default)]
pub(crate) struct BeginReadonlyStatement;

impl surrealdb::opt::IntoQuery for BeginReadonlyStatement {
    fn into_query(self) -> surrealdb::Result<Vec<surrealdb::sql::Statement>> {
        let begin = {
            #[cfg(not(feature = "archodex-com"))]
            {
                surrealdb::sql::statements::BeginStatement::default()
            }
            #[cfg(feature = "archodex-com")]
            {
                archodex_com::begin_readonly_statement()
            }
        };

        Ok(vec![surrealdb::sql::Statement::Begin(begin)])
    }
}

#[instrument(err)]
pub(crate) async fn migrate_service_data_database(
    service_data_surrealdb_url: &str,
    archodex_account_id: &str,
) -> anyhow::Result<()> {
    info!("Migrating service data 'resources' database...");

    // We can migrate using the backend API role and the resource policy set
    // above. But the resource policy can take 30+ seconds to propagate.
    // Instead, we'll use the customer data management role to migrate the
    // database.
    let db = resources_db(service_data_surrealdb_url, archodex_account_id)
        .await
        .context("Failed to get SurrealDB client")?;

    #[cfg(not(feature = "archodex-com"))]
    db.query("DEFINE DATABASE resources;")
        .await?
        .check()
        .context("Failed to define 'resources' SurrealDB database")?;

    migrator::migrate_account_resources_database(&db)
        .await
        .context("Failed to migrate 'resources' database")?;

    info!("Service data SurrealDB Database 'resources' migrated and ready for use");

    Ok(())
}

#[derive(PartialEq)]
enum ArchodexSurrealDatabase {
    Accounts,
    Resources,
}

struct NonconcurrentDBState {
    connection: Surreal<Any>,
    current_database: ArchodexSurrealDatabase,
}

#[instrument(err)]
async fn get_nonconcurrent_db_connection(
    url: &str,
) -> anyhow::Result<&'static Mutex<NonconcurrentDBState>> {
    static NONCONCURRENT_DB: OnceCell<Mutex<NonconcurrentDBState>> = OnceCell::const_new();

    NONCONCURRENT_DB
        .get_or_try_init(|| async {
            let db = surrealdb::engine::any::connect((
                url,
                Config::default()
                    .capabilities(Capabilities::default().with_live_query_notifications(false))
                    .strict(),
            ))
            .await?;

            if let Some(creds) = Env::surrealdb_creds() {
                db.signin(creds)
                    .await
                    .context("Failed to sign in to SurrealDB with SURREALDB_USERNAME and SURREALDB_PASSWORD environment values")?;
            }

            db.use_ns("archodex").use_db("accounts").await?;

            anyhow::Ok(Mutex::new(NonconcurrentDBState { connection: db, current_database: ArchodexSurrealDatabase::Accounts }))
        })
        .await
}

#[instrument(err)]
async fn get_concurrent_db_connection(url: &str) -> anyhow::Result<Surreal<Any>> {
    static ACCOUNTS_DB: OnceCell<Surreal<Any>> = OnceCell::const_new();

    Ok(ACCOUNTS_DB
        .get_or_try_init(|| async {
            let db = surrealdb::engine::any::connect((
                url,
                Config::default()
                    .capabilities(Capabilities::default().with_live_query_notifications(false))
                    .strict(),
            ))
            .await?;

            if let Some(creds) = Env::surrealdb_creds() {
                db.signin(creds)
                    .await
                    .context("Failed to sign in to SurrealDB with SURREALDB_USERNAME and SURREALDB_PASSWORD environment values")?;
            }

            db.use_ns("archodex").use_db("accounts").await?;

            anyhow::Ok(db)
        })
        .await?
        .clone())
}

pub(crate) enum DBConnection {
    Nonconcurrent(tokio::sync::MappedMutexGuard<'static, Surreal<Any>>),
    Concurrent(Surreal<Any>),
}

impl std::ops::Deref for DBConnection {
    type Target = Surreal<Any>;

    fn deref(&self) -> &Self::Target {
        match self {
            DBConnection::Nonconcurrent(db) => db,
            DBConnection::Concurrent(db) => db,
        }
    }
}

#[instrument(err)]
pub(crate) async fn accounts_db() -> Result<DBConnection> {
    #[cfg(feature = "archodex-com")]
    let surrealdb_url = Env::accounts_surrealdb_url();
    #[cfg(not(feature = "archodex-com"))]
    let surrealdb_url = Env::surrealdb_url();

    if !cfg!(feature = "archodex-com") && surrealdb_url.starts_with("rocksdb:") {
        let connection = get_nonconcurrent_db_connection(surrealdb_url).await?;
        let mut db_state = connection.lock().await;

        if db_state.current_database != ArchodexSurrealDatabase::Accounts {
            db_state.connection.use_db("accounts").await?;
            db_state.current_database = ArchodexSurrealDatabase::Accounts;
        }

        Ok(DBConnection::Nonconcurrent(
            tokio::sync::MutexGuard::try_map(db_state, |state| Some(&mut state.connection))
                .unwrap_or_else(|_| unreachable!()),
        ))
    } else {
        Ok(DBConnection::Concurrent(
            get_concurrent_db_connection(surrealdb_url).await?,
        ))
    }
}

#[instrument(err)]
pub(crate) async fn resources_db(
    service_data_surrealdb_url: &str,
    account_id: &str,
) -> anyhow::Result<DBConnection> {
    static DBS_BY_URL: LazyLock<RwLock<HashMap<String, Surreal<Any>>>> =
        LazyLock::new(|| RwLock::new(HashMap::new()));

    if !cfg!(feature = "archodex-com") && service_data_surrealdb_url.starts_with("rocksdb:") {
        let connection = get_nonconcurrent_db_connection(service_data_surrealdb_url).await?;
        let mut db_state = connection.lock().await;

        if db_state.current_database != ArchodexSurrealDatabase::Resources {
            db_state.connection.use_db("resources").await?;
            db_state.current_database = ArchodexSurrealDatabase::Resources;
        }

        Ok(DBConnection::Nonconcurrent(
            tokio::sync::MutexGuard::try_map(db_state, |state| Some(&mut state.connection))
                .unwrap_or_else(|_| unreachable!()),
        ))
    } else {
        let dbs_by_url = DBS_BY_URL.read().await;

        let db = if let Some(db) = dbs_by_url.get(service_data_surrealdb_url) {
            db.clone()
        } else {
            drop(dbs_by_url);

            let mut dbs_by_url = DBS_BY_URL.write().await;

            if let Some(db) = dbs_by_url.get(service_data_surrealdb_url) {
                db.clone()
            } else {
                let db = surrealdb::engine::any::connect((
                    service_data_surrealdb_url,
                    Config::default()
                        .capabilities(Capabilities::default().with_live_query_notifications(false))
                        .strict(),
                ))
                .await?;

                dbs_by_url.insert(service_data_surrealdb_url.to_string(), db.clone());

                db
            }
        };

        if let Some(creds) = Env::surrealdb_creds() {
            db.signin(creds)
                .await
                .with_context(|| format!("Failed to sign in to SurrealDB instance {service_data_surrealdb_url} with SURREALDB_USERNAME and SURREALDB_PASSWORD environment values"))?;
        }

        let namespace = if cfg!(feature = "archodex-com") {
            format!("a{account_id}")
        } else {
            "archodex".to_string()
        };

        db.use_ns(namespace).use_db("resources").await?;

        Ok(DBConnection::Concurrent(db))
    }
}

#[instrument(err, skip_all)]
pub(crate) async fn dashboard_auth_account(
    Extension(auth): Extension<DashboardAuth>,
    Path(params): Path<HashMap<String, String>>,
    mut req: Request,
    next: Next,
) -> Result<Response> {
    let account_id = params
        .get("account_id")
        .expect(":account_id should be in path for dashboard account authentication");

    auth.validate_account_access(account_id).await?;

    let account = accounts_db()
        .await?
        .query(BeginReadonlyStatement)
        .get_account_by_id(account_id.to_owned())
        .query(CommitStatement::default())
        .await?
        .check_first_real_error()?
        .take::<Option<Account>>(0)
        .with_context(|| format!("Failed to get record for account ID {account_id:?}"))?;

    let Some(account) = account else {
        not_found!("Account not found");
    };

    req.extensions_mut().insert(account);

    Ok(next.run(req).await)
}

#[instrument(err, skip_all)]
pub(crate) async fn report_api_key_account(
    Extension(auth): Extension<ReportApiKeyAuth>,
    mut req: Request,
    next: Next,
) -> Result<Response> {
    let account = accounts_db()
        .await?
        .query(BeginReadonlyStatement)
        .get_account_by_id(auth.account_id().to_owned())
        .query(CommitStatement::default())
        .await?
        .check_first_real_error()?
        .take::<Option<Account>>(0)
        .context("Failed to get account record")?;

    let Some(account) = account else {
        not_found!("Account not found");
    };

    auth.validate_account_access(&*(account.resources_db().await?))
        .await?;

    req.extensions_mut().insert(account);

    Ok(next.run(req).await)
}

// Like surrealdb::Response::check, but skips over QueryNotExecuted errors.
// QueryNotExecuted errors are returned for all statements in a transaction
// other than the statement that caused the error. If a transaction fails after
// the first statement, the normal `check()` method will return QueryNotExecuted
// instead of the true cause of the error.
pub(crate) trait QueryCheckFirstRealError {
    #[allow(clippy::result_large_err)]
    fn check_first_real_error(self) -> surrealdb::Result<Self>
    where
        Self: Sized;
}

impl QueryCheckFirstRealError for surrealdb::Response {
    fn check_first_real_error(mut self) -> surrealdb::Result<Self> {
        let errors = self.take_errors();

        if errors.is_empty() {
            return Ok(self);
        }

        if let Some((_, err)) = errors
            .into_iter()
            .filter(|(_, result)| {
                !matches!(
                    result,
                    surrealdb::Error::Db(surrealdb::error::Db::QueryNotExecuted)
                )
            })
            .min_by_key(|(query_num, _)| *query_num)
        {
            return Err(err);
        }

        warn!("Only QueryNotExecuted errors found in response, which shouldn't happen");

        Err(surrealdb::Error::Db(surrealdb::error::Db::QueryNotExecuted))
    }
}
