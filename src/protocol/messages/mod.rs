pub mod list_groups;
pub mod list_offsets;
pub mod metadata;
pub mod offset_fetch;

pub trait MultiTopicResponse {
    type Topic: PerTopicResponse;
    
    fn topics(&self) -> &[Self::Topic];

    fn topic(&self, name: &str) -> Option<&Self::Topic> {
        self.topics().iter()
            .find(|p| p.topic() == name)
    }
    
    fn partition(&self, topic_name: &str, id: i32) -> Option<&<Self::Topic as PerTopicResponse>::Partition> {
        self.topic(topic_name)
            .and_then(|topic| topic.partition(id))
    }
}

pub trait PerTopicResponse {
    type Partition: PerPartitionResponse;

    fn topic(&self) -> &str;
    fn partitions(&self) -> &[Self::Partition];

    fn partition(&self, id: i32) -> Option<&Self::Partition> {
        self.partitions().iter()
            .find(|p| p.partition_id() == id)
    }
}

pub trait PerPartitionResponse {
    fn partition_id(&self) -> i32;
    fn error_code(&self) -> i16;
}