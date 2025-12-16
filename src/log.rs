// This file is part of VarveDB.
//
// Copyright (C) 2025 Matheus Cardoso <varvedb@matheus.sbs>
//
// This Source Code Form is subject to the terms of the Mozilla Public License
// v. 2.0. If a copy of the MPL was not distributed with this file, You can
// obtain one at http://mozilla.org/MPL/2.0/.

//! Logging functionality for VarveDB.

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
// Log Initialization
// ---------------------------------------- //

#[cfg(feature = "log")]
pub fn init(filter: impl AsRef<str>) -> crate::error::LogResult<()> {
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
        super::init("varvedb=trace,info").expect("Failed to initialize logger");
    }
}
