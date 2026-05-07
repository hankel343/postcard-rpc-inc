pub mod tcp_wire;

use tcp_wire::{TcpClientTx, TcpClientRx, TcpClientSpawn};
use icd::{IncrementRequest, IncrementValue};
use postcard_rpc::{header::VarSeqKind, standard_icd::ERROR_PATH};
use tokio::net::TcpStream;
use postcard_rpc::{
    host_client::HostClient,
    standard_icd::WireError,
};


#[tokio::main]
async fn main() {
    let stream = TcpStream::connect("127.0.0.1:8080").await.unwrap();
    let (rx, tx) = stream.into_split();

    let cli = HostClient::<WireError>::new_with_wire(
        TcpClientTx { tx },
        TcpClientRx {rx },
        TcpClientSpawn,
        VarSeqKind::Seq1,
        ERROR_PATH,
        8,
    );

    let resp = cli
    .send_resp::<IncrementValue>(&IncrementRequest { value: 41 })
    .await
    .unwrap();

    println!("Response: {}", resp.value);
}
