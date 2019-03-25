use crate::request::*;

#[derive(Debug, Clone)]
pub struct Request {
    pub topics: Option<Vec<String>>,
    pub allow_auto_topic_creation: bool
}

impl KafkaSerializable for Request {
    fn serialize<W: Write>(&self, out: &mut W)-> Result<()> {
        self.topics.serialize(out)?;
        self.allow_auto_topic_creation.serialize(out)
    }
}

impl KafkaRequest for Request {
    type Response = Response;

    fn api_key() -> i16 {
        3
    }

    fn api_version() -> i16 {
        6
    }
}

#[derive(Debug, Clone)]
pub struct Response {
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

impl KafkaDeserializable for Response {
    fn deserialize<R: Read>(stream: &mut R) -> Result<Self> {
        let throttle_time_ms = i32::deserialize(stream)?;
        let brokers = Vec::<Broker>::deserialize(stream)?;
        let cluster_id = Option::<String>::deserialize(stream)?;
        let controller_id = i32::deserialize(stream)?;
        let topic_metadata = Vec::<TopicMetadata>::deserialize(stream)?;
        Ok(Response {
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