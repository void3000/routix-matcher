use std::sync::Arc;
use tonic::transport::Server;

use crate::app::config::Config;
use crate::db::{
    connection::config::{DatabaseSettings, Settings},
    state::ApplicationState,
};
use crate::services::matcher::handler::Handler;
use crate::services::matcher::model::matcher_server::MatcherServer;

type AppState = ApplicationState;

pub struct MatchServer {
    pub config: Config,
}

impl MatchServer {
    pub fn new(config: Config) -> Self {
        Self { config: config }
    }

    pub async fn start(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        tracing::info!("gRPC server listening on {}", self.config.socket_addr);

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

        let state = Arc::new(AppState::new(&settings, policy_manager_endpoint).await?);

        Server::builder()
            .add_service(MatcherServer::new(Handler::new(state)))
            .serve(self.config.socket_addr)
            .await?;

        Ok(())
    }

    pub async fn shudown(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        Ok(())
    }
}
