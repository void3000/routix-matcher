use tonic::transport::Server;

use crate::app::config::Config;
use crate::app::state::ApplicationState;

use crate::services::matcher::handler::Handler;
use crate::services::matcher::model::matcher_server::MatcherServer;



pub struct MatchServer {
    pub config: Config,
}

impl MatchServer {
    pub fn new(config: Config) -> Self {
        Self { config: config }
    }

    pub async fn start(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        tracing::info!("gRPC server listening on {}", self.config.socket_addr);

        ApplicationState::init().await?;
        let state = ApplicationState::global();

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
