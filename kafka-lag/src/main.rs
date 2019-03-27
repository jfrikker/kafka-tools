extern crate kafka_protocol;

use kafka_protocol::messages::{MultiTopicResponse, PerPartitionResponse};
use kafka_protocol::KafkaConnection;
use kafka_protocol::messages::{list_groups, list_offsets, metadata, offset_fetch};
use std::env::args;
use std::io::Result;

fn main() -> Result<()> {
    let mut addrs: Vec<String> = args().skip(1).collect();
    if addrs.is_empty() {
        addrs = vec!("localhost:9092".into());
    }

    let conns: Result<Vec<KafkaConnection>> = addrs.into_iter()
        .map(|a| KafkaConnection::connect(a))
        .collect();
    let mut conns = conns?;

    let metadata = load_metadata(conns.get_mut(0).unwrap())?;

    let groups: Result<Vec<list_groups::Response>> = conns.iter_mut()
        .map(|c| list_groups(c))
        .collect();
    let groups = groups?.into_iter()
        .filter(|r| !r.groups.is_empty())
        .next()
        .map(|g| g.groups)
        .unwrap_or(Vec::new());
    
    let offsets: Result<Vec<list_offsets::Response>> = conns.iter_mut()
        .map(|c| list_offsets(c, &metadata))
        .collect();
    let offsets = offsets?;

    for group in groups {
        let group_offsets: Result<Vec<offset_fetch::Response>> = conns.iter_mut()
            .map(|c| list_group_offsets(c, group.name.clone(), &metadata))
            .collect();
        let group_offsets = group_offsets?.into_iter()
            .filter(|r| !r.responses.is_empty())
            .next()
            .unwrap();

        for topic in group_offsets.responses.iter() {
            let mut watching = false;
            let mut lag: i64 = 0;
            for partition in topic.partition_responses.iter() {
                let group_offset = partition.offset;
                let max_offset = offsets.iter()
                    .map(|o| o.partition(&topic.topic, partition.partition).unwrap())
                    .filter(|p| p.error_code() == 0)
                    .map(|p| p.offset)
                    .next()
                    .unwrap();

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