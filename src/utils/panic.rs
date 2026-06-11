pub fn set_abort_on_panic() {
    std::panic::set_hook(Box::new(|info| {
        use std::io::Write;

        let backtrace = std::backtrace::Backtrace::force_capture();
        tracing::error!("panic: {info}\n{backtrace}");

        std::io::stderr().flush().ok();
        std::io::stdout().flush().ok();

        std::process::exit(1);
    }));
}
