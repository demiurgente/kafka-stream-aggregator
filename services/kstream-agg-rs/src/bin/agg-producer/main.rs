mod indicators;

use anyhow::Error;
use fehler::throws;
use indicators::EWMA;
use kstream_agg_lib::config::Options;
use kstream_agg_lib::consumer::KafkaConsumer;
use kstream_agg_lib::get_current_ts;
use kstream_agg_lib::models::TradesDataAvro;
use kstream_agg_lib::producer::KafkaProducer;
use kstream_agg_lib::{config::Config, init_tracer};
use lazy_static::lazy_static;
use std::process;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, Mutex};
use tokio::time::Instant;
use tracing::{error, info};

lazy_static! {
    static ref CONFIG: Config = Config::new().unwrap_or_else(|err| {
        error!("Could not parse default config file: {:?}", err);
        process::exit(1);
    });
    static ref KAFKA_CONSUMER: kstream_agg_lib::config::KafkaConsumer =
        CONFIG.kafka.consumer.clone().unwrap();
    static ref KAFKA_CONSUMER_AUTO_OFFSET_RESET: Option<String> =
        KAFKA_CONSUMER.auto_offset_reset.clone();
    static ref KAFKA_CONSUMER_TOPIC_NAME: String = KAFKA_CONSUMER.topic_name.clone();
    static ref KAFKA_GROUP_ID: String = KAFKA_CONSUMER.group_id.clone();
    static ref KAFKA_PRODUCER_TOPIC_NAME: String = CONFIG.kafka.producer.topic_name.clone();
    static ref KAFKA_BROKER_URI: String = CONFIG.kafka.broker.clone();
    static ref KAFKA_SCHEMA_REGISTRY_URI: String = CONFIG.kafka.registry.clone();
    static ref INDICATOR: kstream_agg_lib::config::Indicator = CONFIG.indicator.clone().unwrap();
    static ref INDICATOR_KIND: String = INDICATOR.kind.clone();
    static ref INDICATOR_OPTIONS: Options = INDICATOR.options.clone().unwrap();
    static ref INDICATOR_OPTIONS_PERIOD: i64 = INDICATOR_OPTIONS.period.clone();
    static ref ZIPKIN_URI: String = "http://zipkin-server:9411/api/v2/spans".to_string();
}

#[inline(always)]
pub fn wrap_to_u64(x: i64, s: u64) -> u64 {
    (x as u64).wrapping_mul(s)
}

#[throws(Error)]
#[tokio::main(flavor = "current_thread")]
async fn main() {
    init_tracer(&ZIPKIN_URI);

    info!(
        "Start kafka consumer on server: {}, group_id: {}, topic: {}",
        KAFKA_BROKER_URI.clone(),
        KAFKA_GROUP_ID.clone(),
        KAFKA_CONSUMER_TOPIC_NAME.clone()
    );

    let consumer = KafkaConsumer::new(
        KAFKA_BROKER_URI.clone(),
        KAFKA_SCHEMA_REGISTRY_URI.clone(),
        KAFKA_GROUP_ID.clone(),
        KAFKA_CONSUMER_TOPIC_NAME.clone(),
        KAFKA_CONSUMER_AUTO_OFFSET_RESET.clone(),
    );

    info!(
        "Start kafka producer on server: {}, topic: {}",
        KAFKA_BROKER_URI.clone(),
        KAFKA_PRODUCER_TOPIC_NAME.clone()
    );

    let producer = KafkaProducer::new(
        KAFKA_BROKER_URI.clone(),
        KAFKA_SCHEMA_REGISTRY_URI.clone(),
        KAFKA_PRODUCER_TOPIC_NAME.clone(),
    );

    // Accumulate running events from consumer into shared vector
    let buffer: Vec<f64> = Vec::with_capacity(1000);
    let shared_ctx = Arc::new(Mutex::new(buffer));

    // Refers to aggregate over N minutes (e.g. EWMA over N * 60s events)
    let period_secs = (INDICATOR_OPTIONS_PERIOD.clone() as u64).wrapping_mul(60000); // todo: revisit this conversion
    let start = Instant::now() + Duration::from_millis(period_secs);
    let mut period_interval = tokio::time::interval_at(start, Duration::from_millis(period_secs));

    info!(
        "Accumulate statistic: {}, with interval: {} seconds for {}",
        INDICATOR_KIND.clone(),
        period_secs.clone(),
        KAFKA_CONSUMER_TOPIC_NAME.clone()
    );

    let (sender, mut receiver) = mpsc::unbounded_channel::<TradesDataAvro>();

    let handle = tokio::spawn(async move {
        consumer.consume(sender.clone()).await;
    });

    loop {
        tokio::select! {
            _ = period_interval.tick() => {
                let producer = producer.clone();
                let trace_prefix = format!("Producer for {}:", KAFKA_PRODUCER_TOPIC_NAME.clone());

                let period_n = INDICATOR_OPTIONS_PERIOD.clone();
                let shared_ctx = shared_ctx.clone();

                tokio::spawn(async move {
                    let mut buffer = shared_ctx.lock_owned().await;
                    let mut indicator = EWMA::new(period_n * 60); // adjust period to seconds

                    for v in buffer.iter() {
                        indicator.next(v);
                    }
                    buffer.clear();

                    if indicator.current > 0. {
                        info!("Computed indicator -> {}", indicator);

                        let key = get_current_ts();
                        let payload = indicator;
                        let produce_future = producer.send(key, payload);

                        match produce_future.await {
                            true => info!("{} Sent message successfully", trace_prefix),
                            false => error!("{} Failed to send payload", trace_prefix),
                        }
                    }
                });
            }
            msg = receiver.recv() => {
                let input_topic = KAFKA_CONSUMER_TOPIC_NAME.clone();
                let trace_prefix = format!("Consumer for {}:", input_topic);
                let shared_ctx = shared_ctx.clone();
                match msg {
                    Some(event) => {
                        info!("{} Received payload: {:?}", trace_prefix, event);

                        tokio::task::spawn( async move {
                            let mut buffer = shared_ctx.lock_owned().await;
                            buffer.push(event.price);
                        });
                    },
                    _ => {
                        error!("{} Input channel closed", trace_prefix);
                        break;
                    }
                }
            }
        }
    }
    handle.abort()
}
