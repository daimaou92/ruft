# What?

Learning rust - by implementing the [raft protocol](https://raft.github.io/raft.pdf).

Goes without saying but - please don't use this anywhere.

# Run
```Shell
cargo build
./target/debug/ruft
```

This will start a server with the following default cluster configuration:
1. 127.0.0.1:20000
2. 127.0.0.1:20001
3. 127.0.0.1:20002

This server itself will start on port `:20000`

Open up two new shells and run:

`./target/debug/ruft -p 20001` in one and

`./target/debug/ruft -p 20002` in the other

Watch the logs go and use `Ctrl+C` liberally.

## What works?

The leadership and election bit. There's a leader lease in case only one member is active.

## What doesn't work?

Anything else

# Dependencies:
1. [tokio](https://github.com/tokio-rs/tokio)
2. [tarpc](https://github.com/google/tarpc)
3. [clap](https://github.com/clap-rs/clap)
4. [serde](https://github.com/serde-rs/serde)
5. [anyhow](https://github.com/dtolnay/anyhow)

