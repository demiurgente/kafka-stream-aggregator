# kstream-agg-rs

## Description
Library aims to deliver functionality to work with Kafka streams using Avro, `src/*.rs` files are used for shared behavior. Main services are declared in `src/bin/*`, currently they include:
* `raw_producer` - consume raw events from deribit ws and stream to Kafka topic
* `agg_producer` - performs a calculation on batches of raw data using an exponentially weighted average, streams results to the topic

Shared `src/*.rs` library modules include abstract `Kafka<Consumer/Producer>` that encapsulates initialization, injects context for tracing (debugging, metrics), and makes a dynamic `AvroSchema` on the underlying structure payload. Finally, there is a basic `Config` implementation that allows passing environment variables in a more structured way.

## Service Design
### Raw Event Service (raw_producer)
The algorithm for this service is trivial:
- Establish a connection with the Kafka producer
- Arrange websocket subscription for Deribit handle 
- Poll raw events in a loop
	- on receiving deconstruct payload to list of trades
		- process each trade asynchronously, produce results to Kafka topic

### Aggregate Event Service (agg_producer)
The main difficulty for aggregation comes from the fact that we have to make a calculation each `N` minutes to produce a result, in other words, orchestrating both scheduling aggregation computation and receiving raw messages from Kafka without interruptions is slightly more complex. The approach used:
- Establish connection with raw event Kafka consumer
- Establish a connection with the Kafka producer
- Allocate a temporary `buffer` for raw events
- Calculate the required `interval` in seconds
- Poll both raw events on consumer and `interval`
	- on raw event receive deserialize payload and push it to `buffer`
	- on `interval` calculate a metric using `buffer` and produce to topic


## Docker Build Considerations
[Builds](../../config/raw-producer/Dockerfile) for both services are nearly identical, the only difference is that `raw_producer` requires usage of ca-certificates to make public HTTP requests to establish a subscription, so we have to choose this image over *scratch*:
```
FROM gcr.io/distroless/static-debian12
```

### Intermediate Dependency Caching
A common step for users, but I am a novice in Rust, so it was surprising to accumulate multiple tools that significantly improve dockerized builds. This is achieved by executing
```
cargo install --target $TARGET --path . --bin raw_producer
```
before copying source files to the directory, it will build only libraries that you have declared in `Cargo.toml`.

### Cache Mounts
Allows mount directories that will be locally cached between build, this persistent storage greatly improved download time and intermediate steps during the development
```
RUN --mount=type=cache,target=/usr/local/cargo/registry \
	--mount=type=cache,target=/kstream-agg-rs/target \
```