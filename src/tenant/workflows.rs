///! MIT License
///!
///! Copyright (c) 2025 Keorapetse Finger
///!
///! Permission is hereby granted, free of charge, to any person obtaining a copy
///! of this software and associated documentation files (the "Software"), to deal
///! in the Software without restriction, including without limitation the rights
///! to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
///! copies of the Software, and to permit persons to whom the Software is
///! furnished to do so, subject to the following conditions:
///!
///! The above copyright notice and this permission notice shall be included in all
///! copies or substantial portions of the Software.
///!
///! THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
///! IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
///! FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
///! AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
///! LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
///! OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
///! SOFTWARE.

use async_trait::async_trait;
use lapin::Channel;
use routix_engine::{CoreEngine, engine::lang::ast::Value, models::case::CaseConfig};
use tokio::sync::OnceCell;
use std::{collections::HashMap, sync::Arc};

use crate::{client::MatcherPolicyManagerClient, db::repository::{
    agent::AgentRepository, case::CaseRepository, registry::{
        RegistryEntry, 
        RegistryRepository
    }
}, tenant::{rabbit_mq, shed::{ CaseSummary}}
};

pub struct WorkflowContext {
    pub entry: Option<RegistryEntry>,
    pub skills: Vec<String>,
    pub cases: Vec<CaseConfig>,
    pub policy: Option<String>,
    pub matched_cases: Vec<CaseConfig>,
}

impl Default for WorkflowContext {
    fn default() -> Self {
        Self {
            entry: None,
            skills: vec![],
            cases: vec![],
            policy: None,
            matched_cases: vec![]
        }
    }
}

impl WorkflowContext {
    pub fn agent(&self) -> Result<&RegistryEntry, String> {
        self.entry
            .as_ref()
            .ok_or("agent not loaded".into())
    }

    pub fn policy(&self) -> Result<&str, String> {
        self.policy
            .as_deref()
            .ok_or("policy not loaded".into())
    }
}

#[async_trait]
pub trait WorkflowStep: Send + Sync {
    async fn execute(
        &mut self, 
        ctx: &mut WorkflowContext
    ) -> Result<(), String>;
}

pub struct QueryNextAgentWorkflow {
    pub repo: Arc<RegistryRepository>,
}

pub struct FetchSkillsWorkflow {
    pub repo: Arc<AgentRepository>,
}

pub struct RetrieveCandidateCasesWorkflow {
    pub repo: Arc<CaseRepository>,
}

pub struct ResolvePolicyWorkflow {
    pub client: Arc<MatcherPolicyManagerClient>
}

pub struct EvaluateMatchWorkflow {
    pub engine: CoreEngine,
}

pub struct PushRecommendationWorkflow {
    pub exchange_name: String,
    pub channel: OnceCell<Channel>,
}

pub enum WorkflowType {
    QueryNextAgent(QueryNextAgentWorkflow),
    FetchSkills(FetchSkillsWorkflow),
    RetrieveCandidateCases(RetrieveCandidateCasesWorkflow),
    ResolvePolicy(ResolvePolicyWorkflow),
    EvaluateMatch(EvaluateMatchWorkflow),
    PushRecommendation(PushRecommendationWorkflow)
}

#[async_trait]
impl WorkflowStep for WorkflowType {
    async fn execute(
        &mut self, 
        ctx: &mut WorkflowContext
    ) -> Result<(), String> {
        match self {
            WorkflowType::QueryNextAgent(workflow) => workflow.execute(ctx).await,
            WorkflowType::FetchSkills(workflow) => workflow.execute(ctx).await,
            WorkflowType::RetrieveCandidateCases(workflow) => workflow.execute(ctx).await,
            WorkflowType::ResolvePolicy(workflow) => workflow.execute(ctx).await,
            WorkflowType::EvaluateMatch(workflow) => workflow.execute(ctx).await,
            WorkflowType::PushRecommendation(workflow) => workflow.execute(ctx).await
        }
    }
}

pub struct Workflow {
    pub steps: Vec<WorkflowType>,
}

impl Default for Workflow
{
    fn default() -> Self {
        Self { steps: vec![] }
    }
}

impl Workflow {
    pub async fn run(
        &mut self,
        mut ctx: WorkflowContext
    ) -> Result<WorkflowContext, String> {
        for step in &mut self.steps {
            step.execute(&mut ctx).await?;
        }
        Ok(ctx)
    }

    pub fn add_step(mut self, workflow_step: WorkflowType) -> Self {
        self.steps.push(workflow_step);
        self
    }
}

#[async_trait]
impl WorkflowStep for QueryNextAgentWorkflow {
    async fn execute(
        &mut self,
        ctx: &mut WorkflowContext
    ) -> Result<(), String> 
    {
        ctx.entry = self.repo
            .get_available_agent()
            .await
            .map_err(|e| format!("Failed to poll agent: {}", e))?;
        Ok(())
    }
}

#[async_trait]
impl WorkflowStep for FetchSkillsWorkflow {
    async fn execute(
        &mut self,
        ctx: &mut WorkflowContext
    ) -> Result<(), String> 
    {
        if let Some(agent) = &ctx.entry {
            ctx.skills = self.repo
                .get_agent_skills(&agent.login)
                .await
                .map_err(|_| format!("failed to load skills for agent"))?;
        }
        Ok(())
    }
}

#[async_trait]
impl WorkflowStep for RetrieveCandidateCasesWorkflow {
    async fn execute(
        &mut self,
        ctx: &mut WorkflowContext
    ) -> Result<(), String> 
    {
        if !ctx.skills.is_empty() {
            ctx.cases = self.repo
                .allocate_cases(&ctx.skills)
                .await;
        }
        Ok(())
    }
}

#[async_trait]
impl WorkflowStep for ResolvePolicyWorkflow {
    async fn execute(
        &mut self,
        ctx: &mut WorkflowContext
    ) -> Result<(), String> 
    {
        if let Some(agent) = &ctx.entry {
            ctx.policy = Some(
                self.client
                    .get_policy_data_decoded(&agent.department.clone())
                    .await
                    .map_err(|e| format!("Failed to fetch policy: {}", e))?)
        }
        Ok(())
    }
}

#[async_trait]
impl WorkflowStep for EvaluateMatchWorkflow {
    async fn execute(
        &mut self,
        ctx: &mut WorkflowContext
    ) -> Result<(), String> {
        let source = ctx.policy()
            .map_err(|_| "policy not loaded")?;

        let agent = ctx.agent()
            .map_err(|_| "agent not loaded")?;

        let agent_stack = self.build_agent_stack(agent, &ctx.skills);

        self.engine.set_variable("agent", Value::Map(agent_stack));
        self.engine.add_cases(ctx.cases.clone());
        self.engine.execute_program_from_source(source)?;

        ctx.matched_cases = self.engine.get_cases().to_vec();

        Ok(())
    }
}

impl EvaluateMatchWorkflow {
    fn build_agent_stack(
        &self,
        agent: &RegistryEntry,
        skills: &[String],
    ) -> HashMap<String, Value> {
        HashMap::from([
            (
                "login".into(),
                Value::String(agent.login.clone()),
            ),
            (
                "skills".into(),
                Value::List(
                    skills
                        .iter()
                        .cloned()
                        .map(Value::String)
                        .collect(),
                ),
            ),
            (
                "languages".into(),
                Value::List(vec![]),
            ),
        ])
    }
}

#[async_trait]
impl WorkflowStep for PushRecommendationWorkflow {
    async fn execute(
        &mut self,
        ctx: &mut WorkflowContext,
    ) -> Result<(), String> {
        let agent = ctx.agent()
            .map_err(|_| "agent not loaded")?;

        let summaries: Vec<CaseSummary> = ctx
            .matched_cases
            .iter()
            .map(CaseSummary::from)
            .collect();

        let payload = serde_json::to_vec(&summaries.first())
            .map_err(|e| format!("failed to serialize case summaries: {e}"))?;

        let channel = self.channel
            .get()
            .ok_or("rabbitmq channel not initialized")?;

        let routing_key = format!("agent.{}", agent.login);

        rabbit_mq::publish_case(
            &self.exchange_name,
            channel,
            &routing_key,
            &payload,
        )
        .await;

        Ok(())
    }
}
