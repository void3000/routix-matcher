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

use std::{ sync::Arc };
use tracing::{ info, error };
use tokio::{
    task::JoinHandle,
    sync::OnceCell,
    time::{
        interval,
        Duration
    }
};
use tokio_util::sync::CancellationToken;
use lapin::Channel;
use serde::{ Deserialize, Serialize };

use crate::tenant::workflows::{
    Workflow,
    WorkflowType,
    FetchSkillsWorkflow,
    QueryNextAgentWorkflow,
    ResolvePolicyWorkflow,
    EvaluateMatchWorkflow,
    RetrieveCandidateCasesWorkflow,
    PushRecommendationWorkflow,
    WorkflowContext,
};
use routix_engine::{CoreEngine, models::case::CaseConfig};
use crate::tenant::rabbit_mq;
use crate::app::state::ApplicationState;

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct CaseSummary {
    pub id: i32,
    pub category: String,
    pub status: String,
    pub priority: i32,
}

impl From<&CaseConfig> for CaseSummary {
    fn from(case: &CaseConfig) -> Self {
        Self {
            id: case.id,
            category: case.category.clone(),
            status: case.status.clone(),
            priority: case.priority,
        }
    }
}

/// The Tenant Scheduler
///
/// The scheduler is responsible for prioritizing the next agent to assign a case.
/// It is addtionally, responsible for managing tenant exchange, agent availability
/// status.
pub struct TenantScheduler {
    /// A tenant is a single entiry that my represent a company or a division,
    /// department within a company. A tenant has a one to one mapping with a
    /// workspace. A workspace is a customer visible name for a tenant.
    tenant_id: u32,

    /// An exchange is an entity where the scheduler publishes messages that are
    /// then routed to a set of queues. When a case is found for an agent, it is
    /// published to an exchange. The exhange will be responsible for routing the
    /// case to correct queue owned by an agent.
    exchange_name: String,

    /// Instance of a channel that will keep connection between the scheduler and
    /// message broker instance.
    channel: OnceCell<Channel>,

    /// Message broker endpoint
    /// 
    /// # Example
    /// ```ignore
    /// amqp://guest:guest@127.0.0.1:5672/%2f
    /// ```
    mq_endpoint: String,
    /// The `CancellationToken` is used to gracefully terminate the scheduler loop. 
    /// Calling `shutdown()` will signal the scheduler to stop processing new 
    /// cases and exit its loop.
    shutdown: CancellationToken,
}

/// The schedule handle sould be used by the caller to manage the TenantScheduler
/// process through provided methods such as `shutdown()` method.
pub struct SchedulerHandle {
    pub scheduler: Arc<TenantScheduler>,
    pub handle: JoinHandle<()>,
}

impl TenantScheduler {
    /// Creates a new tenant scheduler, however it does not run it immediately.
    /// 
    /// # Example
    ///
    /// ```ignore
    /// use std::sync::Arc;
    /// use tenant::shed::TenantScheduler;
    /// 
    /// Arc::new(
    ///     TenantScheduler::new(
    ///         13454365,
    ///         "tenant".to_string(),
    ///         "amqp://guest:guest@127.0.0.1:5672/%2f".to_string()
    ///     )
    /// ).run();
    /// ````
    pub fn new(
        tenant_id: u32, 
        exchange_name: String, 
        mq_endpoint: String
    ) -> Self {
        Self {
            tenant_id: tenant_id,
            exchange_name: exchange_name,
            mq_endpoint: mq_endpoint,
            channel: OnceCell::new(),
            shutdown: CancellationToken::new()
        }
    }

    /// The `run` function spins up a loop for the scheduler and returns a JoinHandle
    /// to the calling function. This fexibility not only allows the calling function 
    /// to not care much about what the scehduler is doing, but also manage the
    /// thread: terminate.
    ///
    /// # Example
    /// 
    /// ```ignore
    /// use std::sync::Arc;
    /// use tenant::shed::TenantScheduler;
    ///
    /// let scheduler = Arc::new(TenantScheduler::new(1, "routix".to_string()));
    /// let handle = scheduler.clone().run();
    ///
    /// // Run scheduler for some time...
    ///
    /// scheduler.shutdown();
    /// handle.handle.await.unwrap();
    /// ```
    pub fn run(self: Arc<Self>) -> SchedulerHandle {
        let scheduler =  Arc::clone(&self);
        let handle = tokio::spawn(
            async move {
                scheduler.initialize_channel().await;
                scheduler.scheduler_loop().await; 
            }
        );

        SchedulerHandle {
            scheduler: self,
            handle: handle
        }
    }

    /// The main loop of the tenant scheduler. It continuously checks for new cases 
    /// to assign to agents and publishes them to the appropriate exchange.
    ///
    /// # Example
    ///
    /// ```ignore
    /// use std::sync::Arc;
    /// use tenant::shed::TenantScheduler;
    /// let scheduler = Arc::new(TenantScheduler::new(1, "routix".to_string()));
    /// scheduler.clone().run();
    /// ```
    async fn scheduler_loop(&self) {
        let mut ticker = interval(Duration::from_secs(1));

        loop {
            tokio::select! {
                _ = self.shutdown.cancelled() => {
                    self.tear_adown_channel().await;
                    break;
                },
                _ = ticker.tick() => {
                    self.tick().await;
                }
            }
        }
    }

    async fn tick(&self) {
        info!("Running tenant scheduler: {}", self.tenant_id);

        let state = ApplicationState::global();
        let workflows = vec![
            WorkflowType::QueryNextAgent(QueryNextAgentWorkflow { 
                repo: state.registry_repository.clone()
            }),
            WorkflowType::FetchSkills(FetchSkillsWorkflow { 
                repo: state.agent_repository.clone() 
            }),
            WorkflowType::RetrieveCandidateCases(RetrieveCandidateCasesWorkflow { 
                repo: state.case_repository.clone() 
            }),
            WorkflowType::ResolvePolicy(ResolvePolicyWorkflow { 
                client: state.policy_manager_client.clone() 
            }),
            WorkflowType::EvaluateMatch(EvaluateMatchWorkflow {
                engine: CoreEngine::new()
            }),
            WorkflowType::PushRecommendation(PushRecommendationWorkflow {
                exchange_name: self.exchange_name.clone(),
                channel: self.channel.clone()
            }),
        ];
        
        let _ = Workflow { steps: workflows }
            .run(WorkflowContext::default())
            .await
            .map_err(|e| format!("workflow execution failed: {e}"));
    }
 
    async fn initialize_channel(&self) {
        self.channel.get_or_init(|| async { 
            let channel: Channel =
            rabbit_mq::create_channel(&self.mq_endpoint).await;
            rabbit_mq::create_exchange(
                &self.exchange_name,
                &channel
            ).await;

            channel
        }).await;
    }

    async fn tear_adown_channel(&self) {
        if let Some(channel) = self.channel.get() {
            if let Err(e) = channel.close(200, "shutdown").await {
                error!("Failed to close channel: {:?}", e);
            }
        }
    }

    #[allow(unused_must_use)]
    pub fn shutdown(&self) {
        info!("Shutting down tenant scheduler: {}", self.tenant_id);
        // Tear down channel before exiting. This ensures that all
        // resources are properly released and there are no dangling
        // connections to the message broker.
        self.shutdown.cancel();
    }
}


#[cfg(test)]
mod tenant_scheduler {
    use std::sync::Arc;
    use tokio::time::{ Duration, sleep };
    use crate::tenant::shed::{
        TenantScheduler,
        SchedulerHandle
    };
    use crate::app::state::ApplicationState;

    #[tokio::test]
    async fn run_tenant_scheduler() {
        #[allow(unused_must_use)]
        ApplicationState::init().await;

        init_trace_logging();

        let mut handles: Vec<SchedulerHandle> = Vec::new();

        for tenant_id in 1..=3 {
            let scheduler =  Arc::new(
                TenantScheduler::new(
                    tenant_id, 
                    format!("routix.{}", tenant_id),
                    "amqp://guest:guest@127.0.0.1:5672/%2f".to_string()
                )
            );
            let handler = scheduler.clone().run();

            handles.push(handler);
        }

        sleep(Duration::from_secs(10)).await;

        #[allow(unused_must_use)]
        for handler in &handles {
            handler.scheduler.shutdown();
        }

        #[allow(unused_must_use)]
        for handler in handles {
            handler.handle.await;
        }
    }

    #[allow(unused_must_use)]
    fn init_trace_logging() {
        tracing_subscriber::fmt()
            .with_test_writer()
            .with_env_filter("info")
            .try_init();
    }
}
