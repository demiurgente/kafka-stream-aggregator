pub mod config;
pub mod consumer;
pub mod models;
pub mod producer;
pub mod registry_handler;

use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

pub fn init_tracer(uri: &str) {
    opentelemetry::global::set_text_map_propagator(opentelemetry_zipkin::Propagator::new());
    let tracer = opentelemetry_zipkin::new_pipeline()
        .with_service_name("event_producer".to_owned())
        .with_service_address("127.0.0.1:8080".parse().unwrap())
        .with_collector_endpoint(uri)
        .install_batch(opentelemetry::runtime::Tokio)
        .expect("unable to install zipkin tracer");

    let tracer = tracing_opentelemetry::layer().with_tracer(tracer.clone());
    let subscriber = tracing_subscriber::fmt::layer().json();
    let level = EnvFilter::new("debug".to_owned());

    tracing_subscriber::registry()
        .with(subscriber)
        .with(level)
        .with(tracer)
        .init();
}

#[inline(always)]
pub fn get_current_ts() -> String {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis()
        .to_string()
}
