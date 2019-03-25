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

impl KafkaSerializable for i8 {
    fn serialize<W: Write>(&self, out: &mut W) -> Result<()> {
        out.write_i8(*self)
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

impl KafkaSerializable for i64 {
    fn serialize<W: Write>(&self, out: &mut W) -> Result<()> {
        out.write_i64::<BigEndian>(*self)
    }
}

impl KafkaDeserializable for i64 {
    fn deserialize<R: Read>(stream: &mut R) -> Result<Self> {
        stream.read_i64::<BigEndian>()
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
    fn api_version() -> i16;
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

impl KafkaRequest for MetadataRequest {
    type Response = MetadataResponse;

    fn api_key() -> i16 {
        3
    }

    fn api_version() -> i16 {
        6
    }
}

#[derive(Debug, Clone)]
pub struct MetadataResponse {
    pub throttle_time_ms: i32,
    pub brokers: Vec<Broker>,
    pub cluster_id: Option<String>,
    pub controller_id: i32,
    pub topic_metadata: Vec<TopicMetadata>
}

#[derive(Debug, Clone)]
pub struct Broker {
    pub node_id: i32,
    pub host: String,
    pub port: i32,
    pub rack: Option<String>
}

#[derive(Debug, Clone)]
pub struct TopicMetadata {
    pub error_code: i16,
    pub topic: String,
    pub is_internal: bool,
    pub partition_metadata: Vec<PartitionMetadata>
}

#[derive(Debug, Clone)]
pub struct PartitionMetadata {
    pub error_code: i16,
    pub partition: i32,
    pub leader: i32,
    pub replicas: Vec<i32>,
    pub isr: Vec<i32>,
    pub offline_replicas: Vec<i32>
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

#[derive(Debug, Clone)]
pub struct ListGroupsRequest { }

impl KafkaSerializable for ListGroupsRequest {
    fn serialize<W: Write>(&self, _: &mut W)-> Result<()> {
        Ok(())
    }
}

impl KafkaRequest for ListGroupsRequest {
    type Response = ListGroupsResponse;

    fn api_key() -> i16 {
        16
    }

    fn api_version() -> i16 {
        2
    }
}

#[derive(Debug, Clone)]
pub struct ListGroupsResponse {
    pub throttle_time_ms: i32,
    pub error_code: i16,
    pub groups: Vec<Group>
}

impl KafkaDeserializable for ListGroupsResponse {
    fn deserialize<R: Read>(stream: &mut R) -> Result<Self> {
        let throttle_time_ms = i32::deserialize(stream)?;
        let error_code = i16::deserialize(stream)?;
        let groups = Vec::<Group>::deserialize(stream)?;
        Ok(ListGroupsResponse {
            throttle_time_ms,
            error_code,
            groups
        })
    }
}

#[derive(Debug, Clone)]
pub struct Group {
    pub name: String,
    pub protocol_type: String
}

impl KafkaDeserializable for Group {
    fn deserialize<R: Read>(stream: &mut R) -> Result<Self> {
        let name = String::deserialize(stream)?;
        let protocol_type = String::deserialize(stream)?;
        Ok(Group {
            name,
            protocol_type
        })
    }
}

#[derive(Debug, Clone)]
pub struct ListOffsetsRequest {
    pub replica_id: i32,
    pub isolation_level: i8,
    pub topics: Vec<TopicRequest>
}

#[derive(Debug, Clone)]
pub struct TopicRequest {
    pub topic: String,
    pub partitions: Vec<PartitionRequest>
}

#[derive(Debug, Clone)]
pub struct PartitionRequest {
    pub partition: i32,
    pub timestamp: i64
}

impl KafkaSerializable for ListOffsetsRequest {
    fn serialize<W: Write>(&self, out: &mut W)-> Result<()> {
        self.replica_id.serialize(out)?;
        self.isolation_level.serialize(out)?;
        self.topics.serialize(out)
    }
}

impl KafkaRequest for ListOffsetsRequest {
    type Response = ListOffsetsResponse;

    fn api_key() -> i16 {
        2
    }

    fn api_version() -> i16 {
        3
    }
}

impl KafkaSerializable for TopicRequest {
    fn serialize<W: Write>(&self, out: &mut W)-> Result<()> {
        self.topic.serialize(out)?;
        self.partitions.serialize(out)
    }
}

impl KafkaSerializable for PartitionRequest {
    fn serialize<W: Write>(&self, out: &mut W)-> Result<()> {
        self.partition.serialize(out)?;
        self.timestamp.serialize(out)
    }
}

#[derive(Debug, Clone)]
pub struct ListOffsetsResponse {
    pub throttle_time_ms: i32,
    pub responses: Vec<TopicResponse>
}

#[derive(Debug, Clone)]
pub struct TopicResponse {
    pub topic: String,
    pub partition_responses: Vec<PartitionResponse>
}

#[derive(Debug, Clone)]
pub struct PartitionResponse {
    pub partition: i32,
    pub error_code: i16,
    pub timestamp: i64,
    pub offset: i64
}

impl KafkaDeserializable for ListOffsetsResponse {
    fn deserialize<R: Read>(stream: &mut R) -> Result<Self> {
        let throttle_time_ms = i32::deserialize(stream)?;
        let responses = Vec::<TopicResponse>::deserialize(stream)?;
        Ok(ListOffsetsResponse {
            throttle_time_ms,
            responses
        })
    }
}

impl KafkaDeserializable for TopicResponse {
    fn deserialize<R: Read>(stream: &mut R) -> Result<Self> {
        let topic = String::deserialize(stream)?;
        let partition_responses = Vec::<PartitionResponse>::deserialize(stream)?;
        Ok(TopicResponse {
            topic,
            partition_responses
        })
    }
}

impl KafkaDeserializable for PartitionResponse {
    fn deserialize<R: Read>(stream: &mut R) -> Result<Self> {
        let partition = i32::deserialize(stream)?;
        let error_code = i16::deserialize(stream)?;
        let timestamp = i64::deserialize(stream)?;
        let offset = i64::deserialize(stream)?;
        Ok(PartitionResponse {
            partition,
            error_code,
            timestamp,
            offset
        })
    }
}