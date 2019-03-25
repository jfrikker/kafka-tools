extern crate kafka_protocol;

use kafka_protocol::KafkaConnection;
use kafka_protocol::messages::{list_groups, list_offsets, metadata};
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

fn load_metadata(conn: &mut KafkaConnection) -> Result<metadata::Response> {
    let metadata_req = metadata::Request {
        topics: None,
        allow_auto_topic_creation: false
    };
    conn.send(&metadata_req)
}

fn list_groups(conn: &mut KafkaConnection) -> Result<list_groups::Response> {
    let req = list_groups::Request { };
    conn.send(&req)
}

fn list_offsets(conn: &mut KafkaConnection, metadata: &metadata::Response) -> Result<list_offsets::Response> {
    let topics = metadata.topic_metadata.iter().map(|topic| {
        let partitions = topic.partition_metadata.iter().map(|partition| list_offsets::PartitionRequest {
            partition: partition.partition,
            timestamp: 0
        }).collect();
        list_offsets::TopicRequest {
            topic: topic.topic.clone(),
            partitions
        }
    }).collect();
    let req = list_offsets::Request {
        replica_id: -1,
        isolation_level: 1,
        topics
    };
    conn.send(&req)
}