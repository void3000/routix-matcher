use arc_swap::ArcSwap;
use bb8::Pool;
use std::sync::Arc;

use crate::client::policy_manager::MatcherPolicyManagerClient;
use crate::db::{
    connection::{
        config::Settings,
        manager::SurrealConnectionManager,
        pool::{PoolManager, SurrealPoolManager},
    },
    repository::{case::CaseRepository, repo::Repository},
};

pub type AppState = ApplicationState;

pub struct ApplicationState {
    pub settings: ArcSwap<Settings>,
    pub pool: Arc<Pool<SurrealConnectionManager>>,
    pub case_repository: Arc<CaseRepository>,
    pub policy_manager_client: Arc<MatcherPolicyManagerClient>,
}

impl ApplicationState {
    pub async fn new(
        settings: &Settings,
        policy_manager_endpoint: String,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let db_connection_pool = SurrealPoolManager::create_pool(
            &settings.database.url,
            &settings.database.namespace,
            &settings.database.name,
            settings.database.pool_size,
            &settings.database.username,
            &settings.database.password,
        )
        .await?;

        let pool = Arc::new(db_connection_pool);

        let policy_manager_client = MatcherPolicyManagerClient::new(policy_manager_endpoint)
            .await
            .map_err(|e| format!("Failed to connect to policy manager: {}", e))?;

        Ok(Self {
            settings: ArcSwap::from(Arc::new(settings.clone())),
            pool: Arc::clone(&pool),
            case_repository: Arc::new(CaseRepository::new(pool)),
            policy_manager_client: Arc::new(policy_manager_client),
        })
    }
}
