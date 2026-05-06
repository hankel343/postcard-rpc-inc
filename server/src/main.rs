mod tcp_wire;

use icd::{IncrementRequest, IncrementResponse, IncrementValue};
use postcard_rpc::{
    define_dispatch,
    header::VarHeader,
    server::{
        Dispatch, Server,
        impls::test_channels::{
            ChannelWireRx, ChannelWireSpawn, ChannelWireTx,
            dispatch_impl::{Settings, new_server},
        },
    },
};

use tcp_wire::{TcpWireRx, TcpWireTx, TcpWireSpawn};
use tokio::net::TcpListener;

pub struct ServerContext;

async fn increment_handler(
    _ctx: &mut ServerContext,
    _header: VarHeader,
    req: IncrementRequest,
) -> IncrementResponse {
    println!("I received the request from the client!");
    IncrementResponse {
        value: req.value + 1,
    }
}

define_dispatch! {
  app: IncrementDispatch;
  spawn_fn: spawn_fn;
  tx_impl: TcpWireTx;
  spawn_impl: TcpWireSpawn;
  context: ServerContext;

  endpoints: {
      list: icd::ENDPOINT_LIST;
      | EndpointTy     | kind  | handler           |
      | ---------  | --------  | -------------- |
      | IncrementValue | async | increment_handler |
  };

  topics_in: {
      list: icd::TOPICS_IN_LIST;
      | TopicTy        | kind  | handler   |
      | -------        | ----  | -------   |
  };

  topics_out: {
        list: icd::TOPICS_OUT_LIST;
  };
}

#[tokio::main]
async fn main() {
    let listener = TcpListener::bind("127.0.0.1:8080").await.unwrap();
    println!("Server listening on 127.0.0.1:8080");

    let (stream, addr) = listener.accept().await.unwrap();
    println!("Client connected {addr}");
    let (rx_half, tx_half) = stream.into_split();
    
    let app = IncrementDispatch::new(ServerContext, TcpWireSpawn);
    let kkind = app.min_key_len();

    let mut server = Server::new(
        TcpWireTx::new(tx_half),
        TcpWireRx::new(rx_half),
        vec![0u8; 1027].into_boxed_slice(),
        app,
        kkind,
    );

    server.run().await;
}
