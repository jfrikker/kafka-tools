extern crate byteorder;
extern crate bufstream;

pub mod messages;
pub mod request;

use byteorder::{BigEndian, WriteBytesExt};
use futures::future::{join_all, try_join_all};
use messages::metadata;
use request::{KafkaDeserializable, KafkaRequest};
use std::collections::HashMap;
use std::io::{Cursor, Error, Result};
use tokio::net::{TcpStream, ToSocketAddrs};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

pub struct KafkaConnection {
    conn: TcpStream,
    incoming: Cursor<Vec<u8>>,
    outgoing: Cursor<Vec<u8>>
}

impl KafkaConnection {
    pub async fn connect<A: ToSocketAddrs>(addr: A) -> Result<KafkaConnection> {
        TcpStream::connect(addr).await.map(KafkaConnection::with_stream)
    }

    pub fn with_stream(conn: TcpStream) -> KafkaConnection {
        KafkaConnection {
            conn,
            incoming: Cursor::new(vec!(0; 2 * 1024 * 1024)),
            outgoing: Cursor::new(vec!(0; 2 * 1024 * 1024))
        }
    }

    pub async fn send <R: KafkaRequest>(&mut self, request: &R) -> Result<R::Response> {
        self.send_req(request).await?;
        self.read_resp().await
    }

    async fn send_req <R: KafkaRequest>(&mut self, request: &R) -> Result<()> {
        self.outgoing.set_position(4);
        WriteBytesExt::write_i16::<BigEndian>(&mut self.outgoing, R::api_key())?;
        WriteBytesExt::write_i16::<BigEndian>(&mut self.outgoing, R::api_version())?;
        WriteBytesExt::write_i32::<BigEndian>(&mut self.outgoing, 0)?;
        WriteBytesExt::write_i16::<BigEndian>(&mut self.outgoing, -1)?;
        request.serialize(&mut self.outgoing)?;
        let size = self.outgoing.position() as usize;
        self.outgoing.set_position(0);
        WriteBytesExt::write_i32::<BigEndian>(&mut self.outgoing, (size - 4) as i32)?;
        self.conn.write_all(&self.outgoing.get_ref()[0..size]).await?;
        self.conn.flush().await?;
        Ok(())
    }

    async fn read_resp <R: KafkaDeserializable>(&mut self) -> Result<R> {
        let len = self.conn.read_i32().await?;
        self.incoming.set_position(0);
        self.conn.read_exact(&mut self.incoming.get_mut()[0..(len as usize)]).await?;
        self.incoming.set_position(4);
        let result = R::deserialize(&mut self.incoming)?;
        Ok(result)
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

    pub fn connections(&mut self) -> impl Iterator<Item=&mut KafkaConnection> {
        self.connections.values_mut()
            .filter(|c| c.is_ok())
            .map(|c| c.as_mut().unwrap())
    }

    pub async fn send_all <R: KafkaRequest + Clone>(&mut self, request: &R) -> Result<Vec<R::Response>> {
        try_join_all(self.connections()
            .map(|c| c.send(request))).await
    }

    pub async fn send_any <R: KafkaRequest + Clone>(&mut self, request: &R) -> Result<R::Response> {
        self.connections().next().unwrap().send(request).await
    }
}

async fn load_metadata<I: Iterator<Item=A>, A: ToSocketAddrs>(addrs: I) -> Result<metadata::Response> {
    let mut err: Option<Error> = None;

    for addr in addrs {
        match KafkaConnection::connect(addr).await {
            Ok(mut conn) => return conn.send(&metadata::Request {
                topics: Some(vec!()),
                allow_auto_topic_creation: false
            }).await,
            Err(e) => err = Some(e)
        }
    }

    Err(err.unwrap())
}