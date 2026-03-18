use std::path::PathBuf;

use expressways_protocol::{ControlRequest, ControlResponse, StreamFrame};
use futures_util::{SinkExt, StreamExt};
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpStream;
#[cfg(unix)]
use tokio::net::UnixStream;
use tokio_util::codec::{Framed, LinesCodec};

#[derive(Debug, Clone)]
pub enum Endpoint {
    Tcp(String),
    #[cfg(unix)]
    Unix(PathBuf),
}

#[derive(Debug)]
pub struct Client {
    transport: Transport,
}

#[derive(Debug)]
pub struct StreamClient {
    transport: Transport,
}

#[derive(Debug)]
enum Transport {
    Tcp(Framed<TcpStream, LinesCodec>),
    #[cfg(unix)]
    Unix(Framed<UnixStream, LinesCodec>),
}

#[derive(Debug, Error)]
pub enum ClientError {
    #[error("i/o error: {0}")]
    Io(#[from] std::io::Error),
    #[error("protocol framing error: {0}")]
    Codec(#[from] LinesCodecError),
    #[error("serialization error: {0}")]
    Serialization(#[from] serde_json::Error),
    #[error("server closed the connection")]
    ConnectionClosed,
    #[error("unix sockets are not supported on this platform")]
    UnixUnsupported,
}

type LinesCodecError = tokio_util::codec::LinesCodecError;

impl Client {
    pub async fn connect(endpoint: Endpoint) -> Result<Self, ClientError> {
        match endpoint {
            Endpoint::Tcp(address) => {
                let stream = TcpStream::connect(address).await?;
                stream.set_nodelay(true)?;
                Ok(Self {
                    transport: Transport::Tcp(Framed::new(stream, LinesCodec::new())),
                })
            }
            #[cfg(unix)]
            Endpoint::Unix(path) => {
                let stream = UnixStream::connect(path).await?;
                Ok(Self {
                    transport: Transport::Unix(Framed::new(stream, LinesCodec::new())),
                })
            }
        }
    }

    pub async fn send(&mut self, request: ControlRequest) -> Result<ControlResponse, ClientError> {
        match &mut self.transport {
            Transport::Tcp(transport) => send_request(transport, request).await,
            #[cfg(unix)]
            Transport::Unix(transport) => send_request(transport, request).await,
        }
    }

    pub fn into_stream(self) -> StreamClient {
        StreamClient {
            transport: self.transport,
        }
    }
}

impl StreamClient {
    pub async fn open(&mut self, request: ControlRequest) -> Result<StreamFrame, ClientError> {
        match &mut self.transport {
            Transport::Tcp(transport) => send_stream_open(transport, request).await,
            #[cfg(unix)]
            Transport::Unix(transport) => send_stream_open(transport, request).await,
        }
    }

    pub async fn next_frame(&mut self) -> Result<Option<StreamFrame>, ClientError> {
        match &mut self.transport {
            Transport::Tcp(transport) => read_stream_frame(transport).await,
            #[cfg(unix)]
            Transport::Unix(transport) => read_stream_frame(transport).await,
        }
    }
}

async fn send_request<T>(
    transport: &mut Framed<T, LinesCodec>,
    request: ControlRequest,
) -> Result<ControlResponse, ClientError>
where
    T: AsyncRead + AsyncWrite + Unpin,
{
    let payload = serde_json::to_string(&request)?;
    transport.send(payload).await?;

    let line = transport
        .next()
        .await
        .ok_or(ClientError::ConnectionClosed)??;

    Ok(serde_json::from_str(&line)?)
}

async fn send_stream_open<T>(
    transport: &mut Framed<T, LinesCodec>,
    request: ControlRequest,
) -> Result<StreamFrame, ClientError>
where
    T: AsyncRead + AsyncWrite + Unpin,
{
    let payload = serde_json::to_string(&request)?;
    transport.send(payload).await?;

    let line = transport
        .next()
        .await
        .ok_or(ClientError::ConnectionClosed)??;

    Ok(serde_json::from_str(&line)?)
}

async fn read_stream_frame<T>(
    transport: &mut Framed<T, LinesCodec>,
) -> Result<Option<StreamFrame>, ClientError>
where
    T: AsyncRead + AsyncWrite + Unpin,
{
    match transport.next().await {
        Some(line) => Ok(Some(serde_json::from_str(&line?)?)),
        None => Ok(None),
    }
}
