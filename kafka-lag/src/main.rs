extern crate kafka_protocol;

use kafka_protocol::*;
use std::env::args;
use std::io::Result;

fn main() -> Result<()> {
    let addr = args().nth(1).unwrap_or("localhost:9092".into());
    let mut conn = KafkaConnection::connect(addr)?;
    let metadata = load_metadata(&mut conn)?;
    let groups = list_groups(&mut conn)?;
    println!("{:?}", groups);
    let offsets = list_offsets(&mut conn, &metadata)?;
    println!("{:?}", offsets);
    Ok(())
}

fn load_metadata(conn: &mut KafkaConnection) -> Result<MetadataResponse> {
    let metadata_req = kafka_protocol::MetadataRequest {
        topics: None,
        allow_auto_topic_creation: false
    };
    conn.send(&metadata_req)
}

fn list_groups(conn: &mut KafkaConnection) -> Result<ListGroupsResponse> {
    let req = ListGroupsRequest { };
    conn.send(&req)
}

fn list_offsets(conn: &mut KafkaConnection, metadata: &MetadataResponse) -> Result<ListOffsetsResponse> {
    let topics = metadata.topic_metadata.iter().map(|topic| {
        let partitions = topic.partition_metadata.iter().map(|partition| PartitionRequest {
            partition: partition.partition,
            timestamp: 0
        }).collect();
        TopicRequest {
            topic: topic.topic.clone(),
            partitions
        }
    }).collect();
    let req = ListOffsetsRequest {
        replica_id: -1,
        isolation_level: 1,
        topics
    };
    conn.send(&req)
}