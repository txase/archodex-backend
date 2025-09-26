use serde::{Deserialize, Serialize};
use surrealdb::Uuid;
use tracing::instrument;

use crate::{
    Result,
    account::Account,
    db::{QueryCheckFirstRealError, accounts_db},
    surrealdb_deserializers,
};

#[derive(Clone, Debug, Deserialize, Serialize)]
pub(crate) struct User {
    #[serde(deserialize_with = "surrealdb_deserializers::uuid::deserialize")]
    id: Uuid,
}

impl User {
    pub(crate) fn new(id: Uuid) -> Self {
        Self { id }
    }

    #[instrument(err)]
    pub(crate) async fn ensure_user_record_exists(&self) -> Result<()> {
        accounts_db()
            .await?
            .query("UPSERT $user RETURN NONE")
            .bind(("user", surrealdb::sql::Thing::from(self)))
            .await?
            .check_first_real_error()?;

        Ok(())
    }

    #[cfg(feature = "archodex-com")]
    #[instrument(err)]
    pub(crate) async fn next_account_id(&self) -> Result<String> {
        use crate::env::Env;
        use archodex_error::{anyhow::anyhow, conflict};
        use rand::Rng as _;
        use tracing::info;

        #[derive(Deserialize)]
        struct NumUserAccountsResults {
            num_user_accounts: u32,
        }

        let NumUserAccountsResults { num_user_accounts } = accounts_db()
            .await?
            .query("SELECT COUNT(->has_access->(account WHERE deleted_at IS NONE)) AS num_user_accounts FROM ONLY $user")
            .bind(("user", surrealdb::sql::Thing::from(self)))
            .await?
            .check_first_real_error()?
            .take::<Option<NumUserAccountsResults>>(0)?
            .ok_or_else(|| anyhow!("Failed to query whether user has an account"))?;

        info!(num_user_accounts, "Retrieved number of accounts for user");

        if num_user_accounts >= Env::user_account_limit() {
            conflict!("User account limit exceeded");
        }

        let account_id = rand::thread_rng()
            .gen_range::<u64, _>(1_000_000_000..=9_999_999_999)
            .to_string();

        info!(account_id, "Generated new account ID");

        Ok(account_id)
    }

    pub(crate) async fn list_accounts(&self) -> Result<Vec<Account>> {
        #[derive(Default, Deserialize)]
        struct ListAccountResults {
            accounts: Vec<Account>,
        }

        Ok(accounts_db()
            .await?
            .query("SELECT ->has_access->(account WHERE deleted_at IS NONE).* AS accounts FROM ONLY $user")
            .bind(("user", surrealdb::sql::Thing::from(self)))
            .await?
            .check_first_real_error()?
            .take::<Option<ListAccountResults>>(0)?
            .unwrap_or_default()
            .accounts)
    }
}

impl From<&User> for surrealdb::sql::Thing {
    fn from(user: &User) -> surrealdb::sql::Thing {
        surrealdb::sql::Thing::from((
            "user",
            surrealdb::sql::Id::Uuid(surrealdb::sql::Uuid::from(user.id)),
        ))
    }
}
