// ---------------------------------------- //
// Re-Exports
// ---------------------------------------- //

#[cfg(feature = "log")]
pub mod macros {
    #[cfg(feature = "log_trace")]
    pub use tracing::trace;

    #[cfg(feature = "log_debug")]
    pub use tracing::debug;

    #[cfg(feature = "log_info")]
    pub use tracing::info;

    #[cfg(feature = "log_warn")]
    pub use tracing::warn;

    #[cfg(feature = "log_error")]
    pub use tracing::error;
}

// ---------------------------------------- //
// Error
// ---------------------------------------- //

#[cfg(feature = "log")]
#[derive(thiserror::Error, Debug)]
pub enum LogError {
    #[error(transparent)]
    SetLogger(#[from] tracing_subscriber::util::TryInitError),
}

#[cfg(feature = "log")]
pub type LogResult<T> = Result<T, LogError>;

// ---------------------------------------- //
// Log
// ---------------------------------------- //

#[cfg(feature = "log")]
pub fn init(filter: impl AsRef<str>) -> LogResult<()> {
    use tracing_subscriber::{fmt, layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

    let env_filter = EnvFilter::try_new(filter.as_ref()).unwrap_or_else(|_| EnvFilter::new("info"));
    tracing_subscriber::registry()
        .with(env_filter)
        .with(
            fmt::layer()
                .compact()
                .with_file(true)
                .with_line_number(true)
                .with_thread_ids(true)
                .with_target(true),
        )
        .try_init()
        .map_err(Into::into)
}

// ---------------------------------------- //
// Test
// ---------------------------------------- //

#[cfg(all(test, feature = "log"))]
pub mod test {
    #[rstest::fixture]
    pub fn log_init() {
        super::init("intraradix=trace,info").expect("Failed to initialize logger");
    }
}
