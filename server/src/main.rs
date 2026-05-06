use icd::{IncrementRequest, IncrementResponse, IncrementValue};
use postcard_rpc::{
    define_dispatch,
    header::VarHeader,
    server::impls::test_channels::{
        dispatch_impl::{new_server, Settings},
        ChannelWireRx, ChannelWireSpawn, ChannelWireTx,
    },
};

pub struct ServerContext;

async fn increment_handler(
    _ctx: &mut ServerContext,
    _header: VarHeader,
    req: IncrementRequest,
) -> IncrementResponse {
    println!("I received the request from the client!");
    IncrementResponse{ value: req.value + 1 }
}

define_dispatch! {
    app: IncrementDispatch;
    spawn_fn: spawn_fn;
    tx_impl: ChannelWireTx;
    spawn_impl: ChannelWireSpawn;
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
    let (client_tx, server_rx) = tokio::sync::mpsc::channel(16);
    let (server_tx, client_rx) = tokio::sync::mpsc::channel(16);

    let app = IncrementDispatch::new(ServerContext, ChannelWireSpawn {});
    let kkind = app.min_key_len();

    let mut server = new_server(app, Settings{
        tx: ChannelWireTx::new(server_tx),
        rx: ChannelWireRx::new(server_rx),
        buf: 1024,
        kkind,
    },
);
        
    server.run().await;
}
