extern crate byteorder;
extern crate bufstream;

pub mod messages;
pub mod request;

use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use request::{KafkaDeserializable, KafkaRequest, KafkaSerializable};
use messages::metadata;
use std::collections::HashMap;
use std::io::{Cursor, Error, Result, Write};
use std::net::{TcpStream, ToSocketAddrs};

pub struct KafkaConnection {
    conn: TcpStream,
    outgoing: Cursor<Vec<u8>>
}

impl KafkaConnection {
    pub fn connect<A: ToSocketAddrs>(addr: A) -> Result<KafkaConnection> {
        TcpStream::connect(addr).map(KafkaConnection::with_stream)
    }

    pub fn with_stream(conn: TcpStream) -> KafkaConnection {
        KafkaConnection {
            conn,
            outgoing: Cursor::new(vec!(0; 2 * 1024 * 1024))
        }
    }

    pub fn send <R: KafkaRequest>(&mut self, request: &R) -> Result<R::Response> {
        self.send_req(request)?;
        self.read_resp()
    }

    fn send_req <R: KafkaRequest>(&mut self, request: &R) -> Result<()> {
        self.outgoing.set_position(0);
        self.outgoing.write_i16::<BigEndian>(R::api_key())?;
        self.outgoing.write_i16::<BigEndian>(R::api_version())?;
        self.outgoing.write_i32::<BigEndian>(0)?;
        self.outgoing.write_i16::<BigEndian>(-1)?;
        request.serialize(&mut self.outgoing)?;
        let size = self.outgoing.position() as usize;
        (size as i32).serialize(&mut self.conn)?;
        self.conn.write_all(&self.outgoing.get_ref()[0..size])?;
        self.conn.flush()?;
        Ok(())
    }

    fn read_resp <R: KafkaDeserializable>(&mut self) -> Result<R> {
        self.conn.read_i32::<BigEndian>()?;
        self.conn.read_i32::<BigEndian>()?;
        R::deserialize(&mut self.conn)
    }
}

pub struct KafkaCluster {
    connections: HashMap<(String, i32), Result<KafkaConnection>>
}

impl KafkaCluster {
    pub fn connect<I: Iterator<Item=A>, A: ToSocketAddrs>(addrs: I) -> Result<Self> {
        let metadata = load_metadata(addrs)?;
        let connections = metadata.brokers.iter()
            .map(|b| ((b.host.clone(), b.port), KafkaConnection::connect((&b.host as &str, b.port as u16))))
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

    pub fn send_all <R: KafkaRequest + Clone>(&mut self, request: &R) -> Result<Vec<R::Response>> {
        self.connections()
            .map(|c| c.send(request))
            .collect()
    }

    pub fn send_any <R: KafkaRequest + Clone>(&mut self, request: &R) -> Result<R::Response> {
        self.connections().next().unwrap().send(request)
    }
}

fn load_metadata<I: Iterator<Item=A>, A: ToSocketAddrs>(addrs: I) -> Result<metadata::Response> {
    let mut err: Option<Error> = None;

    for addr in addrs {
        match KafkaConnection::connect(addr) {
            Ok(mut conn) => return conn.send(&metadata::Request {
                topics: Some(vec!()),
                allow_auto_topic_creation: false
            }),
            Err(e) => err = Some(e)
        }
    }

    Err(err.unwrap())
}