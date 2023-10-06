use anyhow::Error;
use deribit::{
    models::{
        subscription::TradesData, PublicSubscribeRequest, SubscriptionData, SubscriptionMessage,
        SubscriptionParams, WithChannel,
    },
    DeribitBuilder,
};
use fehler::throws;
use futures::StreamExt;
use kstream_agg_lib::producer::KafkaProducer;
use kstream_agg_lib::{config::Config, init_tracer};
use kstream_agg_lib::{get_current_ts, models::TradesDataAvro};
use lazy_static::lazy_static;
use tracing::{error, info};

lazy_static! {
    static ref CONFIG: Config = Config::new().unwrap_or_else(|err| {
        error!("Could not parse default config file: {:?}", err);
        std::process::exit(1);
    });
    static ref KAFKA_PRODUCER_TOPIC_NAME: String = CONFIG.kafka.producer.topic_name.clone();
    static ref DERIBIT_REQ_CHANNEL: String = generate_request_channel(&CONFIG);
    static ref KAFKA_BROKER_URI: String = CONFIG.kafka.broker.clone();
    static ref KAFKA_SCHEMA_REGISTRY_URI: String = CONFIG.kafka.registry.clone();
    static ref ZIPKIN_URI: String = "http://zipkin-server:9411/api/v2/spans".to_string();
}

#[inline(always)]
fn generate_request_channel(s: &CONFIG) -> String {
    format!(
        "trades.{}.{}.{}",
        &s.trades.instrument, &s.trades.currency, &s.trades.interval
    )
}

#[throws(Error)]
#[tokio::main(flavor = "current_thread")]
async fn main() {
    init_tracer(&ZIPKIN_URI);

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

    let drb = DeribitBuilder::default()
        .subscription_buffer_size(100000usize)
        .build()
        .unwrap();

    let (mut client, mut subscription) = drb.connect().await?;

    info!("Request deribit websocket: {}", DERIBIT_REQ_CHANNEL.clone());
    let req = PublicSubscribeRequest::new(&[DERIBIT_REQ_CHANNEL.clone()]);

    let _ = client.call(req).await?.await?;

    while let Some(m) = subscription.next().await {
        match m {
            Ok(SubscriptionMessage {
                params:
                    SubscriptionParams::Subscription(SubscriptionData::Trades(WithChannel {
                        channel: _,
                        data: trades,
                    })),
                ..
            }) => {
                let trace_prefix = format!("Producer for {}:", KAFKA_PRODUCER_TOPIC_NAME.clone());
                let futures = trades
                    .iter()
                    .map(|i: &TradesData| {
                        let producer = producer.clone();
                        let trace_prefix = trace_prefix.clone();

                        async move {
                            let event_json = serde_json::to_string_pretty(&i).expect(&format!(
                                "{}: Failed to serialize trade data payload to json string.",
                                trace_prefix
                            ));

                            info!("{}: Received payload: {}", trace_prefix, event_json);

                            let key = get_current_ts();
                            // Converts to local copy of deribit::TradeData that implements AvroSchema serde trait
                            let payload: TradesDataAvro =
                                serde_json::from_str(&event_json).expect(&format!(
                                    "{}: Failed to deserialize to local class from json.",
                                    trace_prefix
                                ));

                            producer.send(key, payload).await;
                        }
                    })
                    .collect::<Vec<_>>();

                for future in futures {
                    info!("{}: Sent status: {:?}", trace_prefix, future.await);
                }
            }
            _ => panic!(),
        }
    }
}
