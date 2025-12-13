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

/// Times the execution of a block and prints the result to stderr.
///
/// This is a convenience wrapper around [`timed!`] that prints timing information
/// in the format: `[varve] {label}: {duration:?}`
///
/// In **release builds**, this macro is a no-op — only the block executes.
///
/// # Arguments
///
/// * `$label` - A displayable label for the operation
/// * `$block` - The code block to time
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
        let __timed_start = ::std::time::Instant::now();
        let __timed_result = $block;
        ::std::eprintln!("[varve] {}: {:?}", $label, __timed_start.elapsed());
        __timed_result
    }};
}

#[macro_export]
#[cfg(not(debug_assertions))]
macro_rules! timed_dbg {
    ($label:expr, $block:expr) => {
        $block
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
