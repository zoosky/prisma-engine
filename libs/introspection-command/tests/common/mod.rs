use std::sync::Mutex;
use log::LevelFilter;

lazy_static! {
    static ref IS_SETUP: Mutex<bool> = Mutex::new(false);
}

pub fn setup() {
    let mut is_setup = IS_SETUP.lock().expect("get IS_SETUP");
    if *is_setup {
        println!("Already set up");
        return;
    }

    println!("Setting up logging");
    let log_level = match std::env::var("TEST_LOG")
        .unwrap_or("warn".to_string())
        .to_lowercase()
        .as_ref()
    {
        "trace" => LevelFilter::Trace,
        "debug" => LevelFilter::Debug,
        "info" => LevelFilter::Info,
        "warn" => LevelFilter::Warn,
        "error" => LevelFilter::Error,
        _ => LevelFilter::Warn,
    };
    fern::Dispatch::new()
        .format(|out, message, record| {
            out.finish(format_args!("[{}][{}] {}", record.target(), record.level(), message))
        })
        .level(log_level)
        .chain(std::io::stdout())
        .apply()
        .expect("fern configuration");

    *is_setup = true;
}
