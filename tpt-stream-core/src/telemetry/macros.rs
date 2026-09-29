/// Emit a [`tracing`](https://docs.rs/tracing) event, when the optional
/// `tracing` feature is enabled.
///
/// Call sites use `tracing`'s own field syntax — `name = value`, the bare
/// shorthand `name`, and a trailing message literal — so the enabled path
/// reads identically to hand-written `tracing` code. The feature-off arm
/// matches the same shapes but expands to nothing, so an instrumented call
/// site costs zero when the feature is off, while a typo in a field name is
/// still a compile error either way.
///
/// The `target` is namespaced per subsystem so a subscriber can filter (e.g.
/// only `tpt_stream_core::httpclient`).
#[cfg(feature = "tracing")]
#[macro_export]
macro_rules! trace_event {
    ($target:literal, $level:expr, $($rest:tt)*) => {
        tracing::event!(target: $target, $level, $($rest)*)
    };
}

/// Feature-off arm: accept the same call shape and expand to nothing, so an
/// instrumented call site has no runtime cost and no dependency.
///
/// `$($rest)*` is forwarded verbatim into a `stringify!`, so the tokens are
/// still parsed (a malformed call is a compile error either way) but no field
/// value is ever evaluated — a `format!` in a field cannot allocate here. The
/// `allow` is needed because the shorthand `rows,` form expands to a binding
/// that this arm never reads.
#[cfg(not(feature = "tracing"))]
#[macro_export]
macro_rules! trace_event {
    ($target:literal, $level:expr, $($rest:tt)*) => {{
        #[allow(unused_variables, unused_imports, unused_mut, clippy::all)]
        let _unused = ($target, $level, stringify!($($rest)*));
    }};
}
