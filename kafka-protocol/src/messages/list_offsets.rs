use crate::request::*;

#[derive(Debug, Clone)]
pub struct Request {
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

impl KafkaSerializable for Request {
    fn serialize<W: Write>(&self, out: &mut W)-> Result<()> {
        self.replica_id.serialize(out)?;
        self.isolation_level.serialize(out)?;
        self.topics.serialize(out)
    }
}

impl KafkaRequest for Request {
    type Response = Response;

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
pub struct Response {
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

impl KafkaDeserializable for Response {
    fn deserialize<R: Read>(stream: &mut R) -> Result<Self> {
        let throttle_time_ms = i32::deserialize(stream)?;
        let responses = Vec::<TopicResponse>::deserialize(stream)?;
        Ok(Response {
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