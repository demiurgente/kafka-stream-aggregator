use config::{ConfigError, Environment, File};
use serde_derive::Deserialize;
use std::env;
use tracing::info;

#[derive(Debug, Deserialize, Clone)]
pub struct Options {
    pub period: i64,
}

#[derive(Debug, Deserialize, Clone)]
pub struct Indicator {
    // can be min, defaults to seconds
    pub options: Option<Options>,
    pub kind: String,
}

#[derive(Debug, Deserialize)]
pub struct Trades {
    pub instrument: String,
    pub currency: String,
    pub interval: String,
}

#[derive(Debug, Deserialize, Clone)]
pub struct KafkaConsumer {
    pub group_id: String,
    pub topic_name: String,
    pub auto_offset_reset: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct KafkaProducer {
    pub topic_name: String,
    pub auto_offset_reset: String,
}

#[derive(Debug, Deserialize)]
pub struct Kafka {
    pub broker: String,
    pub registry: String,
    pub producer: KafkaProducer,
    pub consumer: Option<KafkaConsumer>,
}

#[derive(Debug, Deserialize)]
pub struct Config {
    pub debug: bool,
    pub trades: Trades,
    pub indicator: Option<Indicator>,
    pub kafka: Kafka,
}

impl Config {
    pub fn new() -> Result<Self, ConfigError> {
        let run_mode = env::var("RUN_MODE").unwrap_or_else(|_| "development".into());

        let s = config::Config::builder()
            .add_source(File::with_name("config/default"))
            .add_source(File::with_name(&format!("config/{}", run_mode)).required(false))
            // Eg.. `APP_DEBUG=1 ./target/app` would set the `debug` key
            .add_source(Environment::with_prefix("app"))
            //.set_override("database.url", "postgres://")?
            .build()?;

        info!("Running service with config: {:?}", s);
        s.try_deserialize()
    }
}
