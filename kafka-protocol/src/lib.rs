extern crate byteorder;
extern crate bufstream;

use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use std::io::{Cursor, Read, Result, Write};
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
        self.outgoing.write_i16::<BigEndian>(6)?;
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

pub trait KafkaSerializable {
    fn serialize<W: Write>(&self, out: &mut W)-> Result<()>;
}

pub trait KafkaDeserializable: Sized {
    fn deserialize<R: Read>(stream: &mut R) -> Result<Self>;
}

impl KafkaSerializable for bool {
    fn serialize<W: Write>(&self, out: &mut W) -> Result<()> {
        if *self {
            out.write_all(&[0])?;
        } else {
            out.write_all(&[1])?;
        }
        Ok(())
    }
}

impl KafkaDeserializable for bool {
    fn deserialize<R: Read>(stream: &mut R) -> Result<Self> {
        let mut data = [0 as u8];
        stream.read_exact(&mut data)?;
        Ok(data[0] != 0)
    }
}

impl KafkaSerializable for i16 {
    fn serialize<W: Write>(&self, out: &mut W) -> Result<()> {
        out.write_i16::<BigEndian>(*self)
    }
}

impl KafkaDeserializable for i16 {
    fn deserialize<R: Read>(stream: &mut R) -> Result<Self> {
        stream.read_i16::<BigEndian>()
    }
}

impl KafkaSerializable for i32 {
    fn serialize<W: Write>(&self, out: &mut W) -> Result<()> {
        out.write_i32::<BigEndian>(*self)
    }
}

impl KafkaDeserializable for i32 {
    fn deserialize<R: Read>(stream: &mut R) -> Result<Self> {
        stream.read_i32::<BigEndian>()
    }
}

impl KafkaSerializable for String {
    fn serialize<W: Write>(&self, out: &mut W) -> Result<()> {
        (self.len() as i16).serialize(out)?;
        write!(out, "{}", self)
    }
}

impl KafkaDeserializable for String {
    fn deserialize<R: Read>(stream: &mut R) -> Result<Self> {
        let len = i16::deserialize(stream)?;
        let mut data = vec!(0; len as usize);
        stream.read_exact(&mut data)?;
        Ok(String::from_utf8(data).unwrap())
    }
}

impl KafkaDeserializable for Option<String> {
    fn deserialize<R: Read>(stream: &mut R) -> Result<Self> {
        let len = i16::deserialize(stream)?;
        if len == -1 {
            Ok(None)
        } else {
            let mut data = vec!(0; len as usize);
            stream.read_exact(&mut data)?;
            Ok(Some(String::from_utf8(data).unwrap()))
        }
    }
}

impl <T: KafkaSerializable> KafkaSerializable for Vec<T> {
    fn serialize<W: Write>(&self, out: &mut W) -> Result<()> {
        (self.len() as i32).serialize(out)?;
        for i in self.iter() {
            i.serialize(out)?;
        }
        Ok(())
    }
}

impl <T: KafkaDeserializable> KafkaDeserializable for Vec<T> {
    fn deserialize<R: Read>(stream: &mut R) -> Result<Self> {
        let len = i32::deserialize(stream)?;
        let mut result = Vec::new();
        for _ in 0..len {
            result.push(T::deserialize(stream)?);
        }
        Ok(result)
    }
}

impl <T: KafkaSerializable> KafkaSerializable for Option<Vec<T>> {
    fn serialize<W: Write>(&self, out: &mut W) -> Result<()> {
        match self {
            None => (-1 as i32).serialize(out),
            Some(v) => v.serialize(out)
        }
    }
}

pub trait KafkaRequest: KafkaSerializable {
    type Response: KafkaDeserializable;
    fn api_key() -> i16;
}

#[derive(Debug, Clone)]
pub struct MetadataRequest {
    pub topics: Option<Vec<String>>,
    pub allow_auto_topic_creation: bool
}

impl KafkaSerializable for MetadataRequest {
    fn serialize<W: Write>(&self, out: &mut W)-> Result<()> {
        self.topics.serialize(out)?;
        self.allow_auto_topic_creation.serialize(out)
    }
}

#[derive(Debug, Clone)]
pub struct MetadataResponse {
    throttle_time_ms: i32,
    brokers: Vec<Broker>,
    cluster_id: Option<String>,
    controller_id: i32,
    topic_metadata: Vec<TopicMetadata>
}

#[derive(Debug, Clone)]
pub struct Broker {
    node_id: i32,
    host: String,
    port: i32,
    rack: Option<String>
}

#[derive(Debug, Clone)]
pub struct TopicMetadata {
    error_code: i16,
    topic: String,
    is_internal: bool,
    partition_metadata: Vec<PartitionMetadata>
}

#[derive(Debug, Clone)]
pub struct PartitionMetadata {
    error_code: i16,
    partition: i32,
    leader: i32,
    replicas: Vec<i32>,
    isr: Vec<i32>,
    offline_replicas: Vec<i32>
}

impl KafkaDeserializable for MetadataResponse {
    fn deserialize<R: Read>(stream: &mut R) -> Result<Self> {
        let throttle_time_ms = i32::deserialize(stream)?;
        let brokers = Vec::<Broker>::deserialize(stream)?;
        let cluster_id = Option::<String>::deserialize(stream)?;
        let controller_id = i32::deserialize(stream)?;
        let topic_metadata = Vec::<TopicMetadata>::deserialize(stream)?;
        Ok(MetadataResponse {
            throttle_time_ms,
            brokers,
            cluster_id,
            controller_id,
            topic_metadata
        })
    }
}

impl KafkaDeserializable for Broker {
    fn deserialize<R: Read>(stream: &mut R) -> Result<Self> {
        let node_id = i32::deserialize(stream)?;
        let host = String::deserialize(stream)?;
        let port = i32::deserialize(stream)?;
        let rack = Option::<String>::deserialize(stream)?;
        Ok(Broker {
            node_id,
            host,
            port,
            rack
        })
    }
}

impl KafkaDeserializable for TopicMetadata {
    fn deserialize<R: Read>(stream: &mut R) -> Result<Self> {
        let error_code = i16::deserialize(stream)?;
        let topic = String::deserialize(stream)?;
        let is_internal = bool::deserialize(stream)?;
        let partition_metadata = Vec::<PartitionMetadata>::deserialize(stream)?;
        Ok(TopicMetadata {
            error_code,
            topic,
            is_internal,
            partition_metadata
        })
    }
}

impl KafkaDeserializable for PartitionMetadata {
    fn deserialize<R: Read>(stream: &mut R) -> Result<Self> {
        let error_code = i16::deserialize(stream)?;
        let partition = i32::deserialize(stream)?;
        let leader = i32::deserialize(stream)?;
        let replicas = Vec::<i32>::deserialize(stream)?;
        let isr = Vec::<i32>::deserialize(stream)?;
        let offline_replicas = Vec::<i32>::deserialize(stream)?;
        Ok(PartitionMetadata {
            error_code,
            partition,
            leader,
            replicas,
            isr,
            offline_replicas
        })
    }
}

impl KafkaRequest for MetadataRequest {
    type Response = MetadataResponse;

    fn api_key() -> i16 {
        3
    }
}