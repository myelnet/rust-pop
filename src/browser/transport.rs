use futures::{
    channel::{mpsc, oneshot},
    future::BoxFuture,
    prelude::*,
    stream::BoxStream,
};
use futures_lite::StreamExt;
use js_sys;
use libp2p::core::{
    connection::Endpoint,
    multiaddr::{Multiaddr, Protocol},
    transport::{ListenerEvent, TransportError},
    Transport,
};
use parity_send_wrapper::SendWrapper;
use std::{
    io, mem,
    pin::Pin,
    task::{Context, Poll},
};
use thiserror::Error;
use wasm_bindgen::prelude::*;
use wasm_bindgen::JsCast;
use web_sys::{BinaryType, ErrorEvent, MessageEvent, WebSocket};

#[derive(Debug, PartialEq, Error)]
pub enum Error {
    #[error("{0}")]
    JsError(String),
    #[error("{0}")]
    Other(String),
}

impl From<JsValue> for Error {
    fn from(v: JsValue) -> Self {
        Error::JsError(format!("{:?}", v))
    }
}

#[derive(Clone)]
pub struct WsTransport;

impl WsTransport {
    fn do_dial(
        self,
        maddr: Multiaddr,
        role_override: Endpoint,
    ) -> Result<<Self as Transport>::Dial, TransportError<<Self as Transport>::Error>> {
        let addr = parse_ws_dial_addr(maddr.clone())
            .map_err(|_| TransportError::MultiaddrNotSupported(maddr))?;
        let ws = WebSocket::new(&addr).map_err(|e| TransportError::Other(Error::from(e)))?;

        ws.set_binary_type(BinaryType::Arraybuffer);

        let (s, r) = oneshot::channel();
        let mut sender = Some(s);
        let onopen = Closure::wrap(Box::new(move |_| {
            let sender = sender.take().unwrap();
            drop(sender.send(0));
        }) as Box<dyn FnMut(JsValue)>);
        ws.set_onopen(Some(onopen.as_ref().unchecked_ref()));
        onopen.forget();

        let (mut xs, xr) = mpsc::channel(1000);
        let onmessage = Closure::wrap(Box::new(move |e: MessageEvent| {
            if let Ok(abuf) = e.data().dyn_into::<js_sys::ArrayBuffer>() {
                let array = js_sys::Uint8Array::new(&abuf);
                if !xs.is_closed() {
                    xs.start_send(array.to_vec()).unwrap();
                }
            }
        }) as Box<dyn FnMut(MessageEvent)>);
        ws.set_onmessage(Some(onmessage.as_ref().unchecked_ref()));
        onmessage.forget();

        let web_sock = SendWrapper::new(ws);

        let future = async move {
            match r.await {
                Ok(_) => Ok(Connection {
                    read_state: ConnectionReadState::PendingData(Vec::new()),
                    ws: web_sock,
                    xr,
                }),
                Err(e) => Err(Error::Other(e.to_string())),
            }
        };
        Ok(Box::pin(future))
    }
}

impl Transport for WsTransport {
    type Output = Connection;
    type Error = Error;
    type Listener =
        BoxStream<'static, Result<ListenerEvent<Self::ListenerUpgrade, Self::Error>, Self::Error>>;
    type ListenerUpgrade = BoxFuture<'static, Result<Self::Output, Self::Error>>;
    type Dial = BoxFuture<'static, Result<Self::Output, Self::Error>>;

    fn listen_on(self, addr: Multiaddr) -> Result<Self::Listener, TransportError<Self::Error>> {
        Err(TransportError::Other(Error::Other(
            "Not supported".to_string(),
        )))
    }

    fn dial(self, addr: Multiaddr) -> Result<Self::Dial, TransportError<Self::Error>>
    where
        Self: Sized,
    {
        self.do_dial(addr, Endpoint::Dialer)
    }

    fn dial_as_listener(self, addr: Multiaddr) -> Result<Self::Dial, TransportError<Self::Error>>
    where
        Self: Sized,
    {
        self.do_dial(addr, Endpoint::Listener)
    }

    fn address_translation(&self, _server: &Multiaddr, _observed: &Multiaddr) -> Option<Multiaddr> {
        None
    }
}

/// Reading side of the connection.
enum ConnectionReadState {
    /// Some data have been read and are waiting to be transferred. Can be empty.
    PendingData(Vec<u8>),
    /// An error occurred or an earlier read yielded EOF.
    Finished,
}

pub struct Connection {
    ws: SendWrapper<WebSocket>,
    xr: mpsc::Receiver<Vec<u8>>,
    read_state: ConnectionReadState,
}

impl AsyncRead for Connection {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<Result<usize, io::Error>> {
        loop {
            match mem::replace(&mut self.read_state, ConnectionReadState::Finished) {
                ConnectionReadState::Finished => {
                    break Poll::Ready(Err(io::ErrorKind::BrokenPipe.into()))
                }
                ConnectionReadState::PendingData(mut data) => {
                    log::info!("handling pending data");
                    if data.is_empty() {
                        let mut next_data = match self.xr.poll_next(cx) {
                            Poll::Ready(Some(data)) => data,
                            _ => {
                                self.read_state = ConnectionReadState::PendingData(Vec::new());
                                break Poll::Pending;
                            }
                        };
                        log::info!("read some data");
                        let data_len = next_data.len();
                        if data_len <= buf.len() {
                            if let Ok(()) =
                                std::io::Write::write_all(&mut next_data, &mut buf[..data_len])
                            {
                                log::info!("write data to buffer");
                                self.read_state = ConnectionReadState::PendingData(Vec::new());
                                break Poll::Ready(Ok(data_len));
                            }
                        }
                        log::info!("wrote data to pending buffer");
                        let mut tmp_buf = vec![0; data_len];
                        std::io::Write::write_all(&mut next_data, &mut tmp_buf[..]).unwrap();
                        self.read_state = ConnectionReadState::PendingData(tmp_buf);
                        continue;
                    } else {
                        log::info!("handling pending buffer");
                        if buf.len() <= data.len() {
                            buf.copy_from_slice(&data[..buf.len()]);
                            self.read_state =
                                ConnectionReadState::PendingData(data.split_off(buf.len()));
                            break Poll::Ready(Ok(buf.len()));
                        } else {
                            let len = data.len();
                            buf[..len].copy_from_slice(&data);
                            self.read_state = ConnectionReadState::PendingData(Vec::new());
                            break Poll::Ready(Ok(len));
                        }
                    }
                }
            }
        }
    }
}

impl AsyncWrite for Connection {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, io::Error>> {
        match self.ws.send_with_u8_array(buf) {
            Ok(_) => Poll::Ready(Ok(buf.len())),
            Err(err) => Poll::Ready(Err(io::Error::new(
                io::ErrorKind::Other,
                format!("{:?}", err),
            ))),
        }
    }
    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), io::Error>> {
        // There's no flushing mechanism. In the FFI we consider that writing implicitly flushes.
        Poll::Ready(Ok(()))
    }

    fn poll_close(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), io::Error>> {
        // Shutting down is considered instantaneous.
        match self.ws.close() {
            Ok(()) => Poll::Ready(Ok(())),
            Err(err) => Poll::Ready(Err(io::Error::new(
                io::ErrorKind::Other,
                format!("{:?}", err),
            ))),
        }
    }
}

/// turns a Multiaddr into a valid websocket url
fn parse_ws_dial_addr(addr: Multiaddr) -> Result<String, String> {
    let mut protocols = addr.iter();
    let mut ip = protocols.next();
    let mut tcp = protocols.next();

    let (host_port, dns_name) = loop {
        match (ip, tcp) {
            (Some(Protocol::Ip4(ip)), Some(Protocol::Tcp(port))) => {
                break (format!("{}:{}", ip, port), None)
            }
            (Some(Protocol::Ip6(ip)), Some(Protocol::Tcp(port))) => {
                break (format!("{}:{}", ip, port), None)
            }
            (Some(Protocol::Dns(h)), Some(Protocol::Tcp(port)))
            | (Some(Protocol::Dns4(h)), Some(Protocol::Tcp(port)))
            | (Some(Protocol::Dns6(h)), Some(Protocol::Tcp(port)))
            | (Some(Protocol::Dnsaddr(h)), Some(Protocol::Tcp(port))) => {
                break (format!("{}:{}", &h, port), Some(h))
            }
            (Some(_), Some(p)) => {
                ip = Some(p);
                tcp = protocols.next();
            }
            _ => return Err("Invalid Multiaddr".to_string()),
        }
    };

    let mut protocols = addr.clone();
    let mut p2p = None;
    let (use_tls, path) = loop {
        match protocols.pop() {
            p @ Some(Protocol::P2p(_)) => p2p = p,
            Some(Protocol::Ws(path)) => break (false, path.into_owned()),
            Some(Protocol::Wss(path)) => {
                if dns_name.is_none() {
                    return Err("Invalid Multiaddr".to_string());
                }
                break (true, path.into_owned());
            }
            _ => return Err("Invalid Multiaddr".to_string()),
        }
    };

    let peer = {
        if let Some(proto) = p2p {
            proto.to_string()
        } else {
            "".to_string()
        }
    };

    let addr = {
        if use_tls {
            let name = dns_name.expect("expected tls to have dns name");
            format!("wss://{}{}", name, peer)
        } else {
            format!("ws://{}{}", host_port, peer)
        }
    };
    Ok(addr)
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_parse_address() {
        let maddr: Multiaddr = "/ip4/127.0.0.1/tcp/41505/ws".parse().unwrap();
        let addr = parse_ws_dial_addr(maddr).unwrap();
        println!("addr: {}", addr);

        let maddr: Multiaddr =
            "/ip4/127.0.0.1/tcp/41505/ws/p2p/12D3KooWKi7rwywqtgcD4k5yjsSLqx1mSx9ZHqf5gn9ddP7mapVW"
                .parse()
                .unwrap();
        let addr = parse_ws_dial_addr(maddr).unwrap();
        println!("addr: {}", addr);

        let maddr: Multiaddr =
            "/dns4/frankfurt-xscpu.myel.zone/tcp/443/wss/p2p/12D3KooWR2np9LBSKh31SqbwZVjE7SQTL8xu3wBHqwwKvPsXk6VY"
                .parse()
                .unwrap();
        let addr = parse_ws_dial_addr(maddr).unwrap();
        println!("addr: {}", addr);
    }
}
