use bb8::Pool;
use routix_engine::models::case::CaseConfig;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use surrealdb::sql::Value;
use surrealdb::sql::{Id, Thing};

use crate::db::connection::manager::SurrealConnectionManager;
use crate::db::repository::repo::Repository;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct CaseRecord {
    #[serde(rename = "id")]
    pub id: Thing,
    pub category: String,
    pub channel: Channel,
    pub created_at: DateTime<Utc>,
    pub description: String,
    pub language: String,
    pub priority: i32,
    pub region: String,
    pub status: String,
    pub subject: String,
    pub updated_at: DateTime<Utc>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub customer: Option<String>,

    #[serde(default = "default_score")]
    pub score: i64,

    #[serde(flatten, skip_serializing_if = "Option::is_none")]
    pub extra: Option<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Channel {
    #[serde(rename = "type")]
    pub channel_type: String,
}

fn default_score() -> i64 {
    0
}

pub struct CaseRepository {
    pool: Arc<Pool<SurrealConnectionManager>>,
}

impl Repository for CaseRepository {
    type Pool = Arc<Pool<SurrealConnectionManager>>;

    fn new(pool: Self::Pool) -> Self {
        Self { pool }
    }
}

impl CaseRepository {
    pub async fn allocate_cases(&self, agent_skills: &[String]) -> Vec<CaseConfig> {
        tracing::info!(
            "Fetching cases from the database based on agent skills: {:?}",
            agent_skills
        );

        match self.fetch_cases_from_db(agent_skills).await {
            Ok(cases) => {
                tracing::info!("Successfully fetched {} cases from database", cases.len());
                cases
            }
            Err(e) => {
                tracing::error!(
                    "Error fetching cases from database: {}, falling back to mock data",
                    e
                );
                vec![]
            }
        }
    }

    async fn fetch_cases_from_db(
        &self,
        agent_skills: &[String],
    ) -> Result<Vec<CaseConfig>, Box<dyn std::error::Error + Send + Sync>> {
        let conn = self.pool.get().await?;

        // Build query to filter cases based on agent skills
        let query = if agent_skills.is_empty() {
            "SELECT * FROM cases WHERE status = 'open' LIMIT 10"
        } else {
            "SELECT * FROM cases WHERE status = 'open' AND category IN $skills"
        };

        let mut result = if agent_skills.is_empty() {
            conn.query(query).await?
        } else {
            conn.query(query)
                .bind(("skills", agent_skills.to_vec()))
                .await?
        };

        let cases: Vec<CaseRecord> = result.take(0)?;

        let case_configs: Vec<CaseConfig> = cases
            .into_iter()
            .map(|record| {
                let numeric_id = match record.id.id {
                    Id::Number(n) => n as i32,
                    Id::String(ref s) => s.parse().unwrap_or(0),
                    _ => 0,
                };

                CaseConfig {
                    id: numeric_id,
                    category: record.category,
                    status: record.status,
                    priority: record.priority,
                    customer: record.customer,
                    score: record.score,
                }
            })
            .collect();

        Ok(case_configs)
    }
}
