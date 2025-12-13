// This file is part of VarveDB.
//
// Copyright (C) 2025 Matheus Cardoso <varvedb@matheus.sbs>
//
// This Source Code Form is subject to the terms of the Mozilla Public License
// v. 2.0. If a copy of the MPL was not distributed with this file, You can
// obtain one at http://mozilla.org/MPL/2.0/.

//! Utility macros and helpers for VarveDB.

/// Times the execution of a block and invokes a callback with the label and elapsed duration.
///
/// In **debug builds** (`debug_assertions` enabled), this macro:
/// 1. Records the start time
/// 2. Executes the block
/// 3. Calculates elapsed time
/// 4. Calls the callback with `(label, duration)`
/// 5. Returns the block's result
///
/// In **release builds**, the timing is completely eliminated — only the block executes,
/// with zero overhead.
///
/// # Arguments
///
/// * `$label` - A label (any type accepted by the callback) identifying what is being timed
/// * `$callback` - A closure or function: `FnOnce(label, Duration)`
/// * `$block` - The code block to time
///
/// # Examples
///
/// ```ignore
/// use varvedb::timed;
/// use std::time::Duration;
///
/// let result = timed!("my_operation", |label, dur: Duration| {
///     eprintln!("[{label}] took {dur:?}");
/// }, {
///     // expensive work here
///     42
/// });
/// assert_eq!(result, 42);
/// ```
///
/// Using a named function as callback:
///
/// ```ignore
/// fn log_timing(label: &str, duration: std::time::Duration) {
///     eprintln!("[timing] {}: {:?}", label, duration);
/// }
///
/// let value = timed!("computation", log_timing, {
///     (0..1000).sum::<i32>()
/// });
/// ```
#[macro_export]
#[cfg(debug_assertions)]
macro_rules! timed {
    ($label:expr, $callback:expr, $block:expr) => {{
        let __timed_start = ::std::time::Instant::now();
        let __timed_result = $block;
        ($callback)($label, __timed_start.elapsed());
        __timed_result
    }};
}

#[macro_export]
#[cfg(not(debug_assertions))]
macro_rules! timed {
    ($label:expr, $callback:expr, $block:expr) => {
        $block
    };
}

/// Times the execution of a block and logs the result.
///
/// In **debug builds**, times the block and logs via the selected backend.
/// In **release builds**, this macro is a no-op — only the block executes.
///
/// # Output Backend (priority order)
///
/// - `debug_eprintln`: `eprintln!`
/// - `log_trace`: `tracing::trace!`
/// - `log_debug`: `tracing::debug!`
/// - `log_info`: `tracing::info!`
/// - `log_warn`: `tracing::warn!`
/// - `log_error`: `tracing::error!`
/// - (none): silent, just executes the block
///
/// # Examples
///
/// ```ignore
/// use varvedb::timed_dbg;
///
/// let result = timed_dbg!("serialize", {
///     expensive_serialization()
/// });
/// ```
#[macro_export]
#[cfg(debug_assertions)]
macro_rules! timed_dbg {
    ($label:expr, $block:expr) => {{
        let __start = ::std::time::Instant::now();
        let __result = $block;
        $crate::__varve_log_timing!($label, __start.elapsed());
        __result
    }};
}

#[macro_export]
#[cfg(not(debug_assertions))]
macro_rules! timed_dbg {
    ($label:expr, $block:expr) => {
        $block
    };
}

/// Internal helper macro for timing output dispatch.
#[doc(hidden)]
#[macro_export]
macro_rules! __varve_log_timing {
    ($label:expr, $elapsed:expr) => {
        #[cfg(feature = "debug_eprintln")]
        {
            eprintln!("[varve] {}: {:?}", $label, $elapsed);
        }

        #[cfg(all(not(feature = "debug_eprintln"), feature = "log_trace"))]
        {
            ::tracing::trace!("[varve] {}: {:?}", $label, $elapsed);
        }

        #[cfg(all(
            not(feature = "debug_eprintln"),
            not(feature = "log_trace"),
            feature = "log_debug"
        ))]
        {
            ::tracing::debug!("[varve] {}: {:?}", $label, $elapsed);
        }

        #[cfg(all(
            not(feature = "debug_eprintln"),
            not(feature = "log_trace"),
            not(feature = "log_debug"),
            feature = "log_info"
        ))]
        {
            ::tracing::info!("[varve] {}: {:?}", $label, $elapsed);
        }

        #[cfg(all(
            not(feature = "debug_eprintln"),
            not(feature = "log_trace"),
            not(feature = "log_debug"),
            not(feature = "log_info"),
            feature = "log_warn"
        ))]
        {
            ::tracing::warn!("[varve] {}: {:?}", $label, $elapsed);
        }

        #[cfg(all(
            not(feature = "debug_eprintln"),
            not(feature = "log_trace"),
            not(feature = "log_debug"),
            not(feature = "log_info"),
            not(feature = "log_warn"),
            feature = "log_error"
        ))]
        {
            ::tracing::error!("[varve] {}: {:?}", $label, $elapsed);
        }

        #[cfg(not(any(
            feature = "debug_eprintln",
            feature = "log_trace",
            feature = "log_debug",
            feature = "log_info",
            feature = "log_warn",
            feature = "log_error"
        )))]
        {
            let _ = ($label, $elapsed);
        }
    };
}

/// Executes a block only in debug builds.
///
/// This is useful for debug-only side effects like logging or metrics collection,
/// where the block's return value is not needed.
///
/// In **release builds**, the block is completely eliminated.
///
/// # Examples
///
/// ```ignore
/// use varvedb::debug_only;
///
/// debug_only!({
///     eprintln!("Debug info: buffer size = {}", buffer.len());
///     collect_metrics();
/// });
/// ```
#[macro_export]
#[cfg(debug_assertions)]
macro_rules! debug_only {
    ($block:expr) => {
        $block
    };
}

#[macro_export]
#[cfg(not(debug_assertions))]
macro_rules! debug_only {
    ($block:expr) => {
        ()
    };
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    #[test]
    fn test_timed_returns_block_result() {
        let result = timed!("test", |_: &str, _: Duration| {}, { 42 });
        assert_eq!(result, 42);
    }

    #[test]
    fn test_timed_calls_callback() {
        use std::cell::Cell;

        let called = Cell::new(false);
        let _: () = timed!(
            "test",
            |label: &str, dur: Duration| {
                assert_eq!(label, "test");
                assert!(dur.as_nanos() > 0);
                called.set(true);
            },
            {
                std::thread::sleep(Duration::from_micros(10));
            }
        );

        #[cfg(debug_assertions)]
        assert!(called.get(), "callback should be called in debug builds");
    }

    #[test]
    fn test_timed_dbg_returns_block_result() {
        let result = timed_dbg!("test_op", { "hello" });
        assert_eq!(result, "hello");
    }

    #[test]
    fn test_debug_only_executes_in_debug() {
        use std::cell::Cell;

        let executed = Cell::new(false);
        debug_only!({
            executed.set(true);
        });

        #[cfg(debug_assertions)]
        assert!(executed.get());

        #[cfg(not(debug_assertions))]
        assert!(!executed.get());
    }

    #[test]
    fn test_timed_with_named_function() {
        use std::sync::atomic::{AtomicBool, Ordering};
        static CALLED: AtomicBool = AtomicBool::new(false);

        fn my_callback(_label: &str, _dur: Duration) {
            CALLED.store(true, Ordering::SeqCst);
        }

        let result = timed!("named_fn_test", my_callback, { 123 });
        assert_eq!(result, 123);

        #[cfg(debug_assertions)]
        assert!(CALLED.load(Ordering::SeqCst));
    }
}
