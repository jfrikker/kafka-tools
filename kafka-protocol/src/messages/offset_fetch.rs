use crate::messages::*;
use crate::request::*;

#[derive(Debug, Clone)]
pub struct Request {
    pub group_id: String,
    pub topics: Vec<TopicRequest>
}

#[derive(Debug, Clone)]
pub struct TopicRequest {
    pub topic: String,
    pub partitions: Vec<i32>
}

impl KafkaSerializable for Request {
    fn serialize<W: Write>(&self, out: &mut W)-> Result<()> {
        self.group_id.serialize(out)?;
        self.topics.serialize(out)
    }
}

impl KafkaRequest for Request {
    type Response = Response;

    fn api_key() -> i16 {
        9
    }

    fn api_version() -> i16 {
        4
    }
}

impl KafkaSerializable for TopicRequest {
    fn serialize<W: Write>(&self, out: &mut W)-> Result<()> {
        self.topic.serialize(out)?;
        self.partitions.serialize(out)
    }
}

#[derive(Debug, Clone)]
pub struct Response {
    pub throttle_time_ms: i32,
    pub responses: Vec<TopicResponse>,
    error_code: i16
}

#[derive(Debug, Clone)]
pub struct TopicResponse {
    pub topic: String,
    pub partition_responses: Vec<PartitionResponse>
}

#[derive(Debug, Clone)]
pub struct PartitionResponse {
    pub partition: i32,
    pub offset: i64,
    pub metadata: Option<String>,
    pub error_code: i16
}

impl KafkaDeserializable for Response {
    fn deserialize<R: Read>(stream: &mut R) -> Result<Self> {
        let throttle_time_ms = i32::deserialize(stream)?;
        let responses = Vec::<TopicResponse>::deserialize(stream)?;
        let error_code = i16::deserialize(stream)?;
        Ok(Response {
            throttle_time_ms,
            responses,
            error_code
        })
    }
}

impl MultiTopicResponse for Response {
    type Topic = TopicResponse;

    fn topics(&self) -> &[Self::Topic] {
        &self.responses
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

impl PerTopicResponse for TopicResponse {
    type Partition = PartitionResponse;

    fn topic(&self) -> &str {
        &self.topic
    }

    fn partitions(&self) -> &[Self::Partition] {
        &self.partition_responses
    }
}

impl KafkaDeserializable for PartitionResponse {
    fn deserialize<R: Read>(stream: &mut R) -> Result<Self> {
        let partition = i32::deserialize(stream)?;
        let offset = i64::deserialize(stream)?;
        let metadata = Option::<String>::deserialize(stream)?;
        let error_code = i16::deserialize(stream)?;
        Ok(PartitionResponse {
            partition,
            offset,
            metadata,
            error_code
        })
    }
}

impl PerPartitionResponse for PartitionResponse {
    fn partition_id(&self) -> i32 {
        self.partition
    }

    fn error_code(&self) -> i16 {
        self.error_code
    }
}