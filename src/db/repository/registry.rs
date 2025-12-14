use crate::db::connection::manager::SurrealConnectionManager;
use bb8::Pool;
use serde::{ Deserialize, Serialize };
use tracing::info;
use std::sync::Arc;
use chrono::{ DateTime, Utc, Duration };

#[derive(Debug, Serialize, Deserialize)]
pub enum Status {
    ONLINE,
    OFFLINE,
    AWAY,
    OCCUPIED
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct RegistryEntry {
    pub login: String,
    pub status: Status,
    pub updated_at: DateTime<Utc>,
    pub created_at: DateTime<Utc>,
    pub department: String,
    pub flags: u32,
}

pub struct RegistryRepository {
    pool: Arc<Pool<SurrealConnectionManager>>,
}

impl RegistryRepository {
    pub fn new(pool: Arc<Pool<SurrealConnectionManager>>) -> Self {
        Self { 
            pool
        }
    }

    pub async fn get_available_agent(
        &self,
    ) -> Result<Option<RegistryEntry>, Box<dyn std::error::Error + Send + Sync>> {
        let conn = self.pool.get().await?;

        let mut response = conn
            .query(r#"
                UPDATE registry
                SET updated_at = time::now()
                WHERE status = 'ONLINE'
                AND updated_at < (time::now() - 10s)
            "#)
            .await?;

        info!("DB records: {:?}", response);

        let agent: Option<RegistryEntry> = response.take(0)?;
        Ok(agent)
    }
}
