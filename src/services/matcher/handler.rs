use routix_engine::{CoreEngine, models::case::CaseConfig};
use std::sync::Arc;
use tonic::{Request, Response, Status};

use crate::db::state::ApplicationState;
use crate::services::matcher::model::{Case, WorkRequest, WorkResponse, matcher_server::Matcher};

type AppState = ApplicationState;

pub struct Handler {
    pub state: Arc<AppState>,
}

impl Handler {
    pub fn new(state: Arc<AppState>) -> Self {
        Self { state }
    }
}

#[tonic::async_trait]
impl Matcher for Handler {
    async fn request_work(
        &self,
        request: Request<WorkRequest>,
    ) -> Result<Response<WorkResponse>, Status> {
        let mut engine = CoreEngine::new();

        let request_inner = request.into_inner();

        // Extract agent skills or use empty vector if no agent
        let agent_skills = if let Some(ref agent) = request_inner.agent {
            agent.skills.clone()
        } else {
            Vec::new()
        };

        let cases = self
            .state
            .case_repository
            .allocate_cases(&agent_skills)
            .await;

        engine.add_cases(cases);

        if let Some(agent) = request_inner.agent {
            self.setup_agent_object(&mut engine, &agent);
        } else {
            println!("=== No Agent Information Provided ===");
        }

        // Fetch policy from policy manager
        let department =
            std::env::var("POLICY_DEPARTMENT").unwrap_or_else(|_| "support".to_string());

        let workflow_source = match self
            .state
            .policy_manager_client
            .get_policy_data_decoded(department)
            .await
        {
            Ok(policy_data) => policy_data,
            Err(e) => {
                tracing::error!("Failed to fetch policy from policy manager: {}", e);
                return Err(Status::internal(format!("Policy fetch error: {}", e)));
            }
        };

        tracing::info!("Executing workflow policy from policy manager");

        if let Err(e) = engine.execute_program_from_source(&workflow_source) {
            return Err(Status::internal(format!("EngineError: {}", e)));
        }

        let response = WorkResponse {
            cases: self.filter_and_transform(engine.get_cases()),
        };

        Ok(Response::new(response))
    }
}

impl Handler {
    fn setup_agent_object(
        &self,
        engine: &mut CoreEngine,
        agent: &crate::services::matcher::model::Agent,
    ) {
        use routix_engine::engine::lang::ast::Value;
        use std::collections::HashMap;

        let mut agent_map = HashMap::new();

        agent_map.insert("login".to_string(), Value::String(agent.login.clone()));

        let skills_list = agent
            .skills
            .iter()
            .map(|skill| Value::String(skill.clone()))
            .collect();
        agent_map.insert("skills".to_string(), Value::List(skills_list));

        let languages_list = agent
            .languages
            .iter()
            .map(|lang| Value::String(lang.clone()))
            .collect();
        agent_map.insert("languages".to_string(), Value::List(languages_list));

        engine.set_variable("agent", Value::Map(agent_map));
    }

    fn filter_and_transform(&self, cases: &[CaseConfig]) -> Vec<Case> {
        let score_threshold = std::env::var("SCORE_THRESHOLD")
            .unwrap_or_else(|_| "19".to_string())
            .parse::<i64>()
            .unwrap_or(19);

        cases
            .iter()
            .filter(|c| c.score > score_threshold)
            .map(|c| Case { id: c.id })
            .collect()
    }
}
