use surrealdb::{ Surreal, engine::remote::ws::{ Ws, Client } };
use surrealdb::opt::auth::Root;
use bb8::ManageConnection;

/// A `deadpool::managed::Manager` implementation for managing pooled connections to SurrealDB.
///
/// This allows integration of SurrealDB with the `deadpool` async connection pool,
/// enabling efficient reuse of database connections in an async context.
///
/// # Behavior
/// - `create` establishes a new WebSocket connection to the SurrealDB server.
/// - `recycle` sends a lightweight `"SELECT 1"` query to check if the connection is alive.
///
/// # Example
/// ```rust
/// use deadpool::managed::Pool;
/// let manager = SurrealConnectionManager {
///     endpoint: "localhost:8000".to_string(),
///     namespace: "test".to_string(),
///     database: "default".to_string(),
/// };
/// let pool = Pool::builder(manager).build().unwrap();
/// let conn = pool.get().await?;
/// ```
pub struct SurrealConnectionManager {
    pub endpoint: String,
    pub database: String,
    pub namespace: String,
    pub username: String,
    pub password: String,
}

impl ManageConnection for SurrealConnectionManager {
    type Connection = Surreal<Client>;
    type Error = surrealdb::Error;

    async fn connect(&self) -> Result<Self::Connection, Self::Error> {
        tracing::info!("Connecting to SurrealDB at {} with database auth", self.endpoint);

        let db = Surreal::new::<Ws>(&self.endpoint).await?;

        db.signin(Root {
            username: &self.username,
            password: &self.password,
        }).await?;

        db.use_ns(&self.namespace).use_db(&self.database).await?;

        Ok(db)
    }

    async fn is_valid(&self, conn: &mut Self::Connection) -> Result<(), Self::Error> {
        conn.query("RETURN 1").await?;

        Ok(())
    }

    fn has_broken(&self, _conn: &mut Self::Connection) -> bool {
        false
    }
}
