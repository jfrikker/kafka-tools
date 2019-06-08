use super::protocol::messages::{MultiTopicResponse, PerPartitionResponse};
use super::protocol::{KafkaCluster, KafkaConnection};
use super::protocol::messages::{list_groups, list_offsets, metadata, offset_fetch};
use std::io::Result;

pub fn list_lags(cluster: &mut KafkaCluster) -> Result<()> {
    let metadata = load_metadata(cluster)?;

    let groups = cluster.send_all(&list_groups::Request { })?.into_iter()
        .filter(|r| !r.groups.is_empty())
        .next()
        .map(|g| g.groups)
        .unwrap_or(Vec::new());
    
    let offsets: Result<Vec<list_offsets::Response>> = cluster.connections()
        .map(|c| list_offsets(c, &metadata))
        .collect();
    let offsets = offsets?;

    let topics: Vec<offset_fetch::TopicRequest> = metadata.topic_metadata.iter().map(|topic| {
        let partitions = topic.partition_metadata.iter().map(|partition| partition.partition).collect();
        offset_fetch::TopicRequest {
            topic: topic.topic.clone(),
            partitions
        }
    }).collect();

    for group in groups {
        let group_offsets = cluster.send_all(&offset_fetch::Request {
            group_id: group.name.clone(),
            topics: topics.clone()
        });
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

fn load_metadata(cluster: &mut KafkaCluster) -> Result<metadata::Response> {
    cluster.send_any(&metadata::Request {
        topics: None,
        allow_auto_topic_creation: false
    })
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