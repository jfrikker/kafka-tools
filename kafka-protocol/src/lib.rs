extern crate bytes;

use bytes::{BufMut, BytesMut};
use std::io::{Read, Result, Write};
use std::net::{TcpStream, ToSocketAddrs};

pub struct KafkaConnection {
    conn: TcpStream,
    incoming: BytesMut,
    outgoing: BytesMut
}

impl KafkaConnection {
    pub fn connect<A: ToSocketAddrs>(addr: A) -> Result<KafkaConnection> {
        TcpStream::connect(addr).map(KafkaConnection::with_stream)
    }

    pub fn with_stream(conn: TcpStream) -> KafkaConnection {
        KafkaConnection {
            conn,
            incoming: BytesMut::with_capacity(2 * 1024 * 1024),
            outgoing: BytesMut::with_capacity(2 * 1024 * 1024)
        }
    }

    pub fn send <R: KafkaRequest>(&mut self, request: &R) -> Result<()> {
        R::api_key().serialize(&mut self.outgoing);
        self.outgoing.put_i16_be(7);
        self.outgoing.put_i32_be(0);
        self.outgoing.put_i16_be(-1);
        request.serialize(&mut self.outgoing);
        let req_bytes = self.outgoing.take();
        let size = req_bytes.len() as i32;
        size.serialize(&mut self.outgoing);
        let len_bytes = self.outgoing.take();
        self.conn.write(&len_bytes)?;
        self.conn.write(&req_bytes)?;
        Ok(())
    }
}

pub trait KafkaSerializable {
    fn serialize(&self, buf: &mut BufMut);
}

impl KafkaSerializable for i16 {
    fn serialize(&self, buf: &mut BufMut) {
        buf.put_i16_be(*self);
    }
}

impl KafkaSerializable for i32 {
    fn serialize(&self, buf: &mut BufMut) {
        buf.put_i32_be(*self);
    }
}

impl KafkaSerializable for String {
    fn serialize(&self, buf: &mut BufMut) {
        (self.len() as i16).serialize(buf);
        write!(buf.writer(), "{}", self).unwrap();
    }
}

impl <T: KafkaSerializable> KafkaSerializable for Vec<T> {
    fn serialize(&self, buf: &mut BufMut) {
        (self.len() as i32).serialize(buf);
        for i in self.iter() {
            i.serialize(buf);
        }
    }
}

impl <T: KafkaSerializable> KafkaSerializable for Option<Vec<T>> {
    fn serialize(&self, buf: &mut BufMut) {
        match self {
            None => (-1 as i32).serialize(buf),
            Some(v) => v.serialize(buf)
        };
    }
}

pub trait KafkaRequest: KafkaSerializable {
    fn api_key() -> i16;
}

#[derive(Debug, Clone)]
pub struct MetadataRequest {
    pub topics: Option<Vec<String>>,
    pub allow_auto_topic_creation: bool
}

impl KafkaSerializable for MetadataRequest {
    fn serialize(&self, buf: &mut BufMut) {
        self.topics.serialize(buf);
    }
}

impl KafkaRequest for MetadataRequest {
    fn api_key() -> i16 {
        3
    }
}