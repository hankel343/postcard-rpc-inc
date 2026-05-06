use postcard_rpc::{
    header::{VarHeader, VarKey, VarKeyKind, VarSeq},
    server::{
        AsWireRxErrorKind, AsWireTxErrorKind, WireRx, WireRxErrorKind, WireSpawn, WireTx,
        WireTxErrorKind,
    },
    standard_icd::LoggingTopic,
    Topic,
};
use serde::Serialize;
use std::{
    fmt::Arguments,
    io,
    sync::{
        atomic::{AtomicU32, Ordering},
        Arc,
    },
};
use thiserror::Error;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::tcp::{OwnedReadHalf, OwnedWriteHalf},
    sync::Mutex,
};

// TX
#[derive(Clone)]
pub struct TcpWireTx {
    tx: Arc<Mutex<OwnedWriteHalf>>,
    log_ctr: Arc<AtomicU32>,
}

impl TcpWireTx {
    pub fn new(tx: OwnedWriteHalf) -> Self {
        Self {
            tx: Arc::new(Mutex::new(tx)),
            log_ctr: Arc::new(AtomicU32::new(0)),
        }
    }

    async fn send_frame(&self, frame: Vec<u8>) -> Result<(), TcpWireTxError> {
        let mut guard = self.tx.lock().await;
        guard.write_all(&(frame.len() as u32).to_le_bytes()).await?;
        guard.write_all(&frame).await?;
        Ok(())
    }
}

#[derive(Debug, Error)]
pub enum TcpWireTxError {
    #[error("io error: {0}")]
    Io(#[from] io::Error),
}

impl AsWireTxErrorKind for TcpWireTxError {
    fn as_kind(&self) -> WireTxErrorKind {
        WireTxErrorKind::ConnectionClosed
    }
}

impl WireTx for TcpWireTx {
    type Error = TcpWireTxError;


    async fn send_raw(&self, buf: &[u8]) -> Result<(), Self::Error> {
        self.send_frame(buf.to_vec()).await
    }

    async fn send_log_str(&self, kkind: VarKeyKind, s: &str) -> Result<(), Self::Error> {
        let ctr = self.log_ctr.fetch_add(1, Ordering::Relaxed);
        let key = match kkind {
            VarKeyKind::Key1 => VarKey::Key1(LoggingTopic::TOPIC_KEY1),
            VarKeyKind::Key2 => VarKey::Key2(LoggingTopic::TOPIC_KEY2),
            VarKeyKind::Key4 => VarKey::Key4(LoggingTopic::TOPIC_KEY4),
            VarKeyKind::Key8 => VarKey::Key8(LoggingTopic::TOPIC_KEY),
        };

        let wh = VarHeader {key, seq_no: VarSeq::Seq4(ctr)};
        self.send::<<LoggingTopic as Topic>::Message>(wh, &s.to_string()).await
    }

    async fn send_log_fmt<'a>(
        &self,
        kkind: VarKeyKind,
        a: Arguments<'a>,
    ) -> Result<(), Self::Error> {
        self.send_log_str(kkind, &format!("{a}")).await
    }


    async fn send<T: Serialize + ?Sized>(&self, hdr: VarHeader, msg: &T)
        -> Result<(), Self::Error> {
            let mut frame = hdr.write_to_vec();
            frame.extend(postcard::to_stdvec(msg).unwrap());
            self.send_frame(frame).await
    }
}

// Rx

pub struct TcpWireRx {
    rx: OwnedReadHalf,
}

impl TcpWireRx {
    pub fn new(rx: OwnedReadHalf) -> Self {
        Self {rx}
    }
}

#[derive(Debug, Error)]
pub enum TcpWireRxError {
    #[error("io error: {0}")]
    Io(#[from] io::Error),
    #[error("message too large")]
    TooLarge,
}

impl AsWireRxErrorKind for TcpWireRxError {
    fn as_kind(&self) -> WireRxErrorKind {
        match self {
            TcpWireRxError::Io(_) => WireRxErrorKind::ConnectionClosed,
            TcpWireRxError::TooLarge => WireRxErrorKind::ReceivedMessageTooLarge,
        }
    }
}

impl WireRx for TcpWireRx {
    type Error = TcpWireRxError;

    async fn receive<'a>(&mut self, buf: &'a mut [u8]) -> Result<&'a mut[u8], Self::Error> {
        let mut len_buf = [0u8; 4];
        self.rx.read_exact(&mut len_buf).await?;
        let len = u32::from_le_bytes(len_buf) as usize;
        let out = buf.get_mut(..len).ok_or(TcpWireRxError::TooLarge)?;
        self.rx.read_exact(out).await?;
        Ok(out)
    }
}

// Spawn

#[derive(Clone)]
pub struct TcpWireSpawn;

impl WireSpawn for TcpWireSpawn {
    type Error = core::convert::Infallible;
    type Info = ();

    fn info(&self) -> &Self::Info {
        &()
    }
}
