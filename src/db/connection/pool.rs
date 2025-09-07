use async_trait::async_trait;
use std::time::Duration;

use crate::db::connection::manager::SurrealConnectionManager;

#[async_trait]
pub trait PoolManager {
    type Pool: Send + Sync + 'static;
    type Error;

    async fn create_pool(
        endpoint: &str,
        namespace: &str,
        database: &str,
        pool_size: u32,
        username: &str,
        password: &str
    ) -> Result<Self::Pool, Self::Error>;
}

pub struct SurrealPoolManager;

#[async_trait]
impl PoolManager for SurrealPoolManager {
    type Pool = bb8::Pool<SurrealConnectionManager>;
    type Error = surrealdb::Error;

    async fn create_pool(
        endpoint: &str,
        namespace: &str,
        database: &str,
        pool_size: u32,
        username: &str,
        password: &str
    ) -> Result<Self::Pool, Self::Error> {
        let manager = SurrealConnectionManager {
            endpoint: endpoint.to_string(),
            namespace: namespace.to_string(),
            database: database.to_string(),
            username: username.to_string(),
            password: password.to_string(),
        };

        bb8::Pool
            ::builder()
            .connection_timeout(Duration::from_millis(500))
            .max_size(pool_size)
            .build(manager).await
    }
}
