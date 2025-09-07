use serde::Deserialize;

#[derive(Clone, Deserialize)]
pub struct Settings {
    pub database: DatabaseSettings,
}

#[derive(Clone, Deserialize)]
pub struct DatabaseSettings {
    pub url: String,
    pub namespace: String,
    pub name: String,
    #[serde(default = "default_pool_size")]
    pub pool_size: u32,
    #[serde(default = "default_username")]
    pub username: String,
    #[serde(default = "default_password")]
    pub password: String,
}

fn default_pool_size() -> u32 {
    10
}

fn default_username() -> String {
    "root".to_string()
}

fn default_password() -> String {
    "root".to_string()
}
