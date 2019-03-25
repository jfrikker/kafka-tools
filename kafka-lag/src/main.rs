extern crate kafka_protocol;

use kafka_protocol::messages::MultiTopicResponse;
use kafka_protocol::KafkaConnection;
use kafka_protocol::messages::{list_groups, list_offsets, metadata, offset_fetch};
use std::env::args;
use std::io::Result;

fn main() -> Result<()> {
    let addr = args().nth(1).unwrap_or("localhost:9092".into());
    let mut conn = KafkaConnection::connect(addr)?;
    let metadata = load_metadata(&mut conn)?;
    let groups = list_groups(&mut conn)?;
    let offsets = list_offsets(&mut conn, &metadata)?;

    println!("{:?}", offsets);

    for group in groups.groups.iter() {
        let group_offsets = list_group_offsets(&mut conn, group.name.clone(), &metadata)?;

        for topic in group_offsets.responses.iter() {
            let mut watching = false;
            let mut lag: i64 = 0;
            for partition in topic.partition_responses.iter() {
                let group_offset = partition.offset;
                let max_offset = offsets.partition(&topic.topic, partition.partition).map(|p| p.offset).unwrap_or(0);

                if max_offset > 0 {
                    lag += std::cmp::max(max_offset - group_offset, 0);
                }

                if group_offset >= 0 {
                    watching = true;
                }
            }

            if watching {
                println!("{} / {}: {}", group.name, topic.topic, lag);
            }
        }
    }
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
            timestamp: -1
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

fn list_group_offsets(conn: &mut KafkaConnection, group: String, metadata: &metadata::Response) -> Result<offset_fetch::Response> {
    let topics = metadata.topic_metadata.iter().map(|topic| {
        let partitions = topic.partition_metadata.iter().map(|partition| partition.partition).collect();
        offset_fetch::TopicRequest {
            topic: topic.topic.clone(),
            partitions
        }
    }).collect();
    let req = offset_fetch::Request {
        group_id: group,
        topics
    };
    conn.send(&req)
}