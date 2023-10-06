mod indicators;

use anyhow::Error;
use fehler::throws;
use indicators::EWMA;
use kstream_agg_lib::config::Options;
use kstream_agg_lib::get_current_ts;
use kstream_agg_lib::models::TradesDataAvro;
use kstream_agg_lib::{config::Config, init_tracer};
use lazy_static::lazy_static;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::Consumer;
use rdkafka::consumer::StreamConsumer;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::Message;
use std::process;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::time::Instant;
use tracing::{error, info};

lazy_static! {
    static ref CONFIG: Config = Config::new().unwrap_or_else(|err| {
        error!("Could not parse default config file: {:?}", err);
        process::exit(1);
    });
    static ref KAFKA_CONSUMER: kstream_agg_lib::config::KafkaConsumer =
        CONFIG.kafka.consumer.clone().unwrap();
    static ref KAFKA_CONSUMER_TOPIC_NAME: String = KAFKA_CONSUMER.topic_name.clone();
    static ref KAFKA_GROUP_ID: String = KAFKA_CONSUMER.group_id.clone();
    static ref KAFKA_PRODUCER_TOPIC_NAME: String = CONFIG.kafka.producer.topic_name.clone();
    static ref KAFKA_BROKER_URI: String = CONFIG.kafka.broker.clone();
    static ref INDICATOR: kstream_agg_lib::config::Indicator = CONFIG.indicator.clone().unwrap();
    static ref INDICATOR_KIND: String = INDICATOR.kind.clone();
    static ref INDICATOR_OPTIONS: Options = INDICATOR.options.clone().unwrap();
    static ref INDICATOR_OPTIONS_PERIOD: u64 = INDICATOR_OPTIONS.period.clone();
    static ref INDICATOR_OPTIONS_PERIOD_SECONDS: u64 = INDICATOR_OPTIONS_PERIOD.clone() * 60;
    static ref ZIPKIN_URI: String = "http://zipkin-server:9411/api/v2/spans".to_string();
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

    let consumer: StreamConsumer = ClientConfig::new()
        .set("group.id", KAFKA_GROUP_ID.clone())
        .set("bootstrap.servers", KAFKA_BROKER_URI.clone())
        .set("enable.partition.eof", "false")
        .set("session.timeout.ms", "6000")
        .set("enable.auto.commit", "false")
        .create()
        .expect("Consumer creation failed");

    consumer
        .subscribe(&[&KAFKA_CONSUMER_TOPIC_NAME])
        .expect("Can't subscribe to specified topic");

    info!(
        "Start kafka producer on server: {}, topic: {}",
        KAFKA_BROKER_URI.clone(),
        KAFKA_PRODUCER_TOPIC_NAME.clone()
    );

    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", KAFKA_BROKER_URI.clone())
        .set("message.timeout.ms", "5000")
        .create()
        .expect("Producer creation error");

    // Accumulate running events from consumer into shared vector
    let buffer: Vec<f64> = Vec::with_capacity(1000);
    let shared_ctx = Arc::new(Mutex::new(buffer));

    // Refers to aggregate over 1 minute
    // (e.g. EWMA over 1 min events from collected buffer over 1 min)
    let period_secs = INDICATOR_OPTIONS_PERIOD.clone() * 60000;
    let start = Instant::now() + Duration::from_millis(period_secs);
    let mut period_interval = tokio::time::interval_at(start, Duration::from_millis(period_secs));

    info!(
        "Accumulate statistic: {}, with interval: {} seconds for {}",
        INDICATOR_KIND.clone(),
        INDICATOR_OPTIONS_PERIOD_SECONDS.clone(),
        KAFKA_CONSUMER_TOPIC_NAME.clone()
    );

    loop {
        tokio::select! {
            _ = period_interval.tick() => {
                let producer = producer.clone();
                let trace_prefix = format!("Producer for {}:", KAFKA_PRODUCER_TOPIC_NAME.clone());

                let shared_ctx =shared_ctx.clone();
                let period_n = INDICATOR_OPTIONS_PERIOD.clone();

                tokio::spawn(async move {
                    let mut buffer = shared_ctx.lock_owned().await;
                    let mut ema = EWMA::new(period_n);

                    for t in buffer.iter() {
                        ema.next(t);
                    }

                    info!("Computed indicator {} -> {}", INDICATOR_KIND.clone(), ema.current);
                    buffer.clear();

                    let output_topic = KAFKA_PRODUCER_TOPIC_NAME.clone();
                    let key = get_current_ts();
                    let payload = serde_json::to_string_pretty(&ema)
                        .expect(&format!("{} Failed to serialize payload to json.", trace_prefix));

                    let produce_future = producer.send(
                        FutureRecord::to(&output_topic)
                            .key(&key)
                            .payload(&payload),
                        Duration::from_secs(0),
                    );

                    match produce_future.await {
                        Ok((partition, commit)) => info!("{} Sent message for partition {} with offset {} successfully", trace_prefix, partition, commit),
                        Err((e, _)) => error!("{}: Failed to send payload. Error: {:?}", trace_prefix, e),
                    }
                });
            }
        message = consumer.recv() => {
                match message {
                    Ok(msg) => {
                        let shared_ctx = shared_ctx.clone();
                        let input_topic = KAFKA_CONSUMER_TOPIC_NAME.clone();

                        let trace_prefix = format!("Consumer for {}:", input_topic);
                        info!("{} Received message on partition: {} with offset: {}", trace_prefix, msg.partition(), msg.offset());

                        match msg.payload_view::<str>() {
                            Some(Ok(payload)) => {
                                let event: TradesDataAvro = serde_json::from_str(payload)
                                    .expect(&format!("{}: Failed to deserialize payload from json.", trace_prefix));

                                info!("{} Received payload: {:?}", trace_prefix, event);

                                tokio::task::spawn( async move {
                                    let mut vec = shared_ctx.lock_owned().await;
                                    vec.push(event.price);
                                });
                            },
                            Some(Err(e)) => info!("{} Message payload is not a string {:?}", trace_prefix, e),
                            None => info!("{} No payload received", trace_prefix),
                        }
                    },
                _ => break
                }
            }
        }
    }
}
