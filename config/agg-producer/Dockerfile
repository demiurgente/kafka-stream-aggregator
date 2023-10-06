FROM rust:1.72 as build

ARG TARGET=x86_64-unknown-linux-musl
ENV CARGO_TERM_COLOR always

RUN apt-get update && \
	apt-get -y install cmake musl-tools

RUN rustup target add $TARGET
RUN ln -s "/usr/bin/g++" "/usr/bin/musl-g++"

# Create a new empty shell project
RUN USER=root cargo new kstream-agg-rs
WORKDIR /kstream-agg-rs

COPY ./Cargo.lock ./Cargo.toml ./

# Step removes code for binary declaration
RUN mkdir -p src/bin/raw-producer \
		     src/bin/agg-producer && \
	touch src/lib.rs && \
  	echo "fn main() {}" | tee \
    	src/bin/raw-producer/main.rs \
    	src/bin/agg-producer/main.rs

# Build only the dependencies to cache them
RUN --mount=type=cache,target=/usr/local/cargo/registry \
	--mount=type=cache,target=/kstream-agg-rs/target \
 	cargo install --target $TARGET --path . --bin agg_producer  && \
	rm target/$TARGET/release/deps/kstream_agg* \
	   target/$TARGET/release/deps/libkstream_* \
	   target/$TARGET/release/deps/agg_prod*

# Build for release.
COPY src ./src

RUN --mount=type=cache,target=/usr/local/cargo/registry \
	--mount=type=cache,target=/kstream-agg-rs/target \
	cargo install --target $TARGET --path . --bin agg_producer

FROM scratch
COPY --from=build /usr/local/cargo/bin/agg_producer .

CMD ["./agg_producer"]
