use tracing_subscriber::{ fmt, EnvFilter };

pub fn init() {
    fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"))
        )
        .init();
}
