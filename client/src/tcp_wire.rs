use icd::{IncrementRequest, IncrementValue};
use postcard_rpc::{
    header::VarSeqKind,
    host_client::{HostClient, WireRx, WireSpawn, WireTx},
    standard_icd::{WireError, ERROR_PATH},
};
use std::io;
use thiserror::Error;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{
        tcp::{OwnedReadHalf, OwnedWriteHalf},
        TcpStream,
    },
};

//
// Transport impls
// 

pub struct TcpClientTx {
    pub tx: OwnedWriteHalf,
}

#[derive(Debug, Error)]
pub enum TcpClientError {
    #[error("io error: {0}")]
    Io(#[from] io::Error),
}

impl WireTx for TcpClientTx {
    type Error = TcpClientError;

    async fn send(&mut self, data: Vec<u8>) -> Result<(), Self::Error> {
        self.tx.write_all(&(data.len() as u32).to_le_bytes()).await?;
        self.tx.write_all(&data).await?;
        Ok(())
    }
}

pub struct TcpClientRx {
    pub rx: OwnedReadHalf,
}

impl WireRx for TcpClientRx {
    type Error = TcpClientError;

    async fn receive(&mut self) -> Result<Vec<u8>, Self::Error> {
        let mut len_buf = [0u8; 4];
        self.rx.read_exact(&mut len_buf).await?;
        let len = u32::from_le_bytes(len_buf) as usize;
        let mut frame = vec![0u8; len];
        self.rx.read_exact(&mut frame).await?;
        Ok(frame)
    }
}

pub struct TcpClientSpawn;

impl WireSpawn for TcpClientSpawn {
    fn spawn(&mut self, fut: impl Future<Output = ()> + Send + 'static) {
        tokio::task::spawn(fut);
    }
}
