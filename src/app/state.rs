use arc_swap::ArcSwap;
use bb8::Pool;
use std::sync::Arc;
use tokio::sync::OnceCell;

use crate::client::policy_manager::MatcherPolicyManagerClient;
use crate::db::{
    connection::{
        config::Settings,
        manager::SurrealConnectionManager,
        pool::{PoolManager, SurrealPoolManager},
    },
    repository::{case::CaseRepository, agent::AgentRepository, registry::RegistryRepository, repo::Repository},
};

static STATE: OnceCell<Arc<ApplicationState>> = OnceCell::const_new();

pub struct ApplicationState {
    pub settings: ArcSwap<Settings>,
    pub pool: Arc<Pool<SurrealConnectionManager>>,
    pub case_repository: Arc<CaseRepository>,
    pub agent_repository: Arc<AgentRepository>,
    pub registry_repository: Arc<RegistryRepository>,
    pub policy_manager_client: Arc<MatcherPolicyManagerClient>,
}

use crate::db::connection::config::DatabaseSettings;

impl ApplicationState {
    pub async fn init() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if STATE.get().is_some() {
            return Ok(());
        }

        let surrealdb_url =
            std::env::var("SURREALDB_URL").unwrap_or_else(|_| "127.0.0.1:8000".to_string());

        let surrealdb_namespace =
            std::env::var("SURREALDB_NAMESPACE").unwrap_or_else(|_| "test_ns".to_string());

        let surrealdb_database =
            std::env::var("SURREALDB_DATABASE").unwrap_or_else(|_| "test_db".to_string());

        let surrealdb_username =
            std::env::var("SURREALDB_USERNAME").unwrap_or_else(|_| "root".to_string());

        let surrealdb_password =
            std::env::var("SURREALDB_PASSWORD").unwrap_or_else(|_| "root".to_string());

        let pool_size = std::env::var("SURREALDB_POOL_SIZE")
            .unwrap_or_else(|_| "10".to_string())
            .parse::<u32>()
            .unwrap_or(10);

        let policy_manager_endpoint = std::env::var("POLICY_MANAGER_ENDPOINT")
            .unwrap_or_else(|_| "http://localhost:50081".to_string());

        let settings = Settings {
            database: DatabaseSettings {
                url: surrealdb_url,
                namespace: surrealdb_namespace,
                name: surrealdb_database,
                pool_size,
                username: surrealdb_username,
                password: surrealdb_password,
            },
        };

        let state = Self::create(&settings, policy_manager_endpoint).await?;
        if STATE.set(Arc::new(state)).is_err() {
            return Err("Failed to initialize global ApplicationState".into());
        }
        Ok(())
    }

    pub fn global() -> Arc<ApplicationState> {
        STATE.get().expect("ApplicationState is not initialized").clone()
    }

    async fn create(
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
            case_repository: Arc::new(CaseRepository::new(Arc::clone(&pool))),
            agent_repository: Arc::new(AgentRepository::new(Arc::clone(&pool))),
            registry_repository: Arc::new(RegistryRepository::new(Arc::clone(&pool))),
            policy_manager_client: Arc::new(policy_manager_client),
        })
    }
}
