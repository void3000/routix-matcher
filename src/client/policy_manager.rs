use std::sync::Arc;
use tokio::sync::Mutex;

// Re-export types for convenience
pub use routix_rust_clients::PolicyManagerClient;
use routix_rust_clients::clients::policy_manager::proto::{GetPolicyRequest, GetPolicyResponse};

#[derive(Clone)]
pub struct MatcherPolicyManagerClient {
    inner: Arc<Mutex<PolicyManagerClient>>,
}

impl MatcherPolicyManagerClient {
    pub async fn new(endpoint: String) -> Result<Self, routix_rust_clients::ClientError> {
        let inner = PolicyManagerClient::new(endpoint).await?;
        Ok(Self {
            inner: Arc::new(Mutex::new(inner)),
        })
    }

    /// Get policy (delegates to inner client)
    pub async fn get_policy(
        &self,
        request: GetPolicyRequest,
    ) -> Result<GetPolicyResponse, routix_rust_clients::ClientError> {
        let mut client = self.inner.lock().await;
        client.get_policy(request).await
    }

    /// Get policy data decoded (matcher-specific convenience method)
    pub async fn get_policy_data_decoded(
        &self,
        department: &str,
    ) -> Result<String, routix_rust_clients::ClientError> {
        let mut client = self.inner.lock().await;
        let (_, decoded_data) = client.get_policy_decoded(department.to_string()).await?;
        Ok(decoded_data)
    }

    /// Get policy with version and decoded data
    pub async fn get_policy_decoded(
        &self,
        department: String,
    ) -> Result<(String, String), routix_rust_clients::ClientError> {
        let mut client = self.inner.lock().await;
        client.get_policy_decoded(department).await
    }
}

// For backward compatibility
pub type Client = MatcherPolicyManagerClient;
