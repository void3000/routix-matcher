use surrealdb::sql::Thing;
use bb8::Pool;
use serde::{ Deserialize, Serialize };
use std::sync::Arc;

use crate::db::connection::manager::SurrealConnectionManager;

pub struct AgentRepository {
    pool: Arc<Pool<crate::db::connection::manager::SurrealConnectionManager>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentConfigRecord {
    #[serde(rename = "id")]
    pub id: Thing,

    pub agent: AgentDetails,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentDetails {
    pub login: String,
    pub languages: Vec<String>,
    pub skills: Vec<String>,
}

impl AgentRepository {
    pub fn new(pool: Arc<Pool<SurrealConnectionManager>>) -> Self {
        Self { pool }
    }

    pub async fn get_agent_skills(
        &self,
        agent_id: &str,
    ) -> Result<Vec<String>, Box<dyn std::error::Error + Send + Sync>> {
        let conn = self.pool.get().await?;

        let mut response = conn
            .query(r"SELECT * FROM agents WHERE login = $login")
            .bind(("agent_id", agent_id.to_string()))
            .await?;

        let result: Option<AgentConfigRecord> = response.take(0)?;
        
        Ok(result.map(|r| r.agent.skills).unwrap_or_default())
    }
}
