extern crate byteorder;
extern crate bufstream;

pub mod messages;
pub mod request;

use byteorder::{BigEndian, WriteBytesExt};
use futures::future::{FutureExt, join_all, try_join_all};
use messages::metadata;
use request::{KafkaDeserializable, KafkaRequest};
use std::collections::HashMap;
use std::io::{Cursor, Error, Result};
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpStream, ToSocketAddrs};
use tokio::sync::Mutex;

type ResponseHandler = Box<dyn Send + FnOnce(&mut Cursor<Vec<u8>>) -> Result<()>>;

struct InFlightRequests {
    next_correlation_id: i32,
    response_handlers: HashMap<i32, ResponseHandler>
}

impl InFlightRequests {
    fn new() -> Self {
        InFlightRequests {
            next_correlation_id: 1,
            response_handlers: HashMap::new()
        }
    }

    fn register_handler<R: KafkaDeserializable + Send + 'static>(&mut self) -> (i32, tokio::sync::oneshot::Receiver<R>) {
        let correlation_id = self.next_correlation_id;
        self.next_correlation_id += 1;
        let (sender, receiver) = tokio::sync::oneshot::channel();
        self.response_handlers.insert(correlation_id, Box::new(move |incoming| {
            let result = R::deserialize(incoming)?;
            sender.send(result).unwrap();
            Ok(())
        }));
        (correlation_id, receiver)
    }

    fn get_handler(&mut self, correlation_id: i32) -> ResponseHandler {
        self.response_handlers.remove(&correlation_id).unwrap()
    }
}

struct WriteHalf {
    conn: tokio::io::WriteHalf<TcpStream>,
    outgoing: Cursor<Vec<u8>>
}

impl WriteHalf {
    fn new(conn: tokio::io::WriteHalf<TcpStream>) -> Self {
        WriteHalf {
            conn,
            outgoing: Cursor::new(vec!(0; 2 * 1024 * 1024))
        }
    }

    async fn send_req<R: KafkaRequest>(&mut self, request: &R, correlation_id: i32) -> Result<()> {
        self.outgoing.set_position(4);
        WriteBytesExt::write_i16::<BigEndian>(&mut self.outgoing, R::api_key())?;
        WriteBytesExt::write_i16::<BigEndian>(&mut self.outgoing, R::api_version())?;
        WriteBytesExt::write_i32::<BigEndian>(&mut self.outgoing, correlation_id)?;
        WriteBytesExt::write_i16::<BigEndian>(&mut self.outgoing, -1)?;
        request.serialize(&mut self.outgoing)?;
        let size = self.outgoing.position() as usize;
        self.outgoing.set_position(0);
        WriteBytesExt::write_i32::<BigEndian>(&mut self.outgoing, (size - 4) as i32)?;
        self.conn.write_all(&self.outgoing.get_ref()[0..size]).await?;
        self.conn.flush().await?;
        Ok(())
    }
}

pub struct KafkaConnection {
    write: Arc<Mutex<WriteHalf>>,
    in_flight: Arc<Mutex<InFlightRequests>>
}

async fn read_loop(mut read: tokio::io::ReadHalf<TcpStream>, in_flight: Arc<Mutex<InFlightRequests>>) -> Result<()> {
    let mut incoming: Cursor<Vec<u8>> = Cursor::new(vec!(0; 2 * 1024 * 1024));
    loop {
        let len = read.read_i32().await?;
        let correlation_id = read.read_i32().await?;
        incoming.set_position(0);
        read.read_exact(&mut incoming.get_mut()[0..((len - 4) as usize)]).await?;
        in_flight.lock().await.get_handler(correlation_id)(&mut incoming)?;
    }
}

impl KafkaConnection {
    pub async fn connect<A: ToSocketAddrs>(addr: A) -> Result<KafkaConnection> {
        TcpStream::connect(addr).await.map(KafkaConnection::with_stream)
    }

    pub fn with_stream(conn: TcpStream) -> KafkaConnection {
        let (read, write) = tokio::io::split(conn);
        let in_flight = Arc::new(Mutex::new(InFlightRequests::new()));
        let in_flight_copy = in_flight.clone();
        tokio::spawn(
            read_loop(read, in_flight_copy).map(|res| {
                match res {
                    Ok(_) => {},
                    Err(e) => {
                        if e.kind() != tokio::io::ErrorKind::UnexpectedEof {
                            eprintln!("Error reading from connection: {}", e);
                        }
                    }
                }
            })
        );

        KafkaConnection {
            write: Arc::new(Mutex::new(WriteHalf::new(write))),
            in_flight
        }
    }

    pub async fn send <R: KafkaRequest>(&self, request: &R) -> Result<R::Response> {
        let (correlation_id, recv) = self.in_flight.lock().await.register_handler();
        self.write.lock().await.send_req(request, correlation_id).await?;
        Ok(recv.await.unwrap())
    }
}

pub struct KafkaCluster {
    connections: HashMap<(String, i32), Result<KafkaConnection>>
}

impl KafkaCluster {
    pub async fn connect<I: Iterator<Item=A>, A: ToSocketAddrs>(addrs: I) -> Result<Self> {
        let metadata = load_metadata(addrs).await?;
        let connections = join_all(metadata.brokers.iter()
            .map(|b| KafkaConnection::connect((&b.host as &str, b.port as u16)))).await;

        let connections = metadata.brokers.iter().zip(connections.into_iter())
            .map(|(addr, conn)| ((addr.host.clone(), addr.port), conn))
            .collect();

        Ok(KafkaCluster {
            connections
        })
    }

    pub fn is_completely_connected(&self) -> bool {
        !self.connections.values().any(|c| c.is_err())
    }

    pub fn connections(&self) -> impl Iterator<Item=&KafkaConnection> {
        self.connections.values()
            .filter(|c| c.is_ok())
            .map(|c| c.as_ref().unwrap())
    }

    pub async fn send_all <R: KafkaRequest + Clone>(&self, request: &R) -> Result<Vec<R::Response>> {
        try_join_all(self.connections()
            .map(|c| c.send(request))).await
    }

    pub async fn send_any <R: KafkaRequest + Clone>(&self, request: &R) -> Result<R::Response> {
        self.connections().next().unwrap().send(request).await
    }
}

async fn load_metadata<I: Iterator<Item=A>, A: ToSocketAddrs>(addrs: I) -> Result<metadata::Response> {
    let mut err: Option<Error> = None;

    for addr in addrs {
        match KafkaConnection::connect(addr).await {
            Ok(conn) => return conn.send(&metadata::Request {
                topics: Some(vec!()),
                allow_auto_topic_creation: false
            }).await,
            Err(e) => err = Some(e)
        }
    }

    Err(err.unwrap())
}