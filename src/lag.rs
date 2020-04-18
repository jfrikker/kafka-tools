use super::protocol::messages::{MultiTopicResponse, PerPartitionResponse};
use super::protocol::{KafkaCluster, KafkaConnection};
use super::protocol::messages::{list_groups, list_offsets, metadata, offset_fetch};
use futures::try_join;
use futures::future::try_join_all;
use std::io::Result;

pub async fn list_lags(cluster: &KafkaCluster) -> Result<()> {
    let (metadata, groups) = try_join!(load_metadata(cluster), load_groups(cluster))?;
    let offsets = load_offsets(cluster, &metadata).await?;

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
        }).await;
        let group_offsets = group_offsets?.into_iter()
            .find(|r| !r.responses.is_empty())
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

async fn load_metadata(cluster: &KafkaCluster) -> Result<metadata::Response> {
    cluster.send_any(&metadata::Request {
        topics: None,
        allow_auto_topic_creation: false
    }).await
}

async fn load_groups(cluster: &KafkaCluster) -> Result<Vec<list_groups::Group>> {
    Ok(cluster.send_all(&list_groups::Request { }).await?.into_iter()
        .find(|r| !r.groups.is_empty())
        .map(|g| g.groups)
        .unwrap_or_default())
}

async fn load_offsets(cluster: &KafkaCluster, metadata: &metadata::Response) -> Result<Vec<list_offsets::Response>> {
    try_join_all(cluster.connections()
        .map(|c| list_offsets(c, &metadata))).await
}

async fn list_offsets(conn: &KafkaConnection, metadata: &metadata::Response) -> Result<list_offsets::Response> {
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
    conn.send(&req).await
}