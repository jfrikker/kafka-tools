use crate::protocol::KafkaCluster;
use crate::protocol::messages::metadata;
use itertools::join;
use std::collections::BTreeSet;
use std::io::Result;
use std::process::exit;

pub async fn cluster_health(cluster: &mut KafkaCluster) -> Result<()> {
    let metadata = load_metadata(cluster).await?;

    let mut brokers_up_to_date = true;

    for broker in metadata.brokers.iter() {
        let mut broker_up_to_date = true;
        println!("Broker {}:{}", broker.host, broker.port);
        for topic in metadata.topic_metadata.iter() {
            for partition in topic.partition_metadata.iter() {
                if !partition.replicas.contains(&broker.node_id) {
                    continue;
                }

                if !partition.isr.contains(&broker.node_id) {
                    println!("  WARN: Catching up on topic {} partition {}", topic.topic, partition.partition);
                    broker_up_to_date = false;
                }
            }
        }

        if broker_up_to_date {
            println!(" IN SYNC!");
        }

        brokers_up_to_date = brokers_up_to_date && broker_up_to_date;
    }

    let offline_brokers: BTreeSet<i32> = metadata.topic_metadata.iter()
        .flat_map(|topic| topic.partition_metadata.iter())
        .flat_map(|partition| partition.offline_replicas.iter().cloned())
        .collect();

    if !offline_brokers.is_empty() {
        println!("ERR: The following brokers are offline: {}", join(&offline_brokers, ", "))
    }

    let mut partitions_have_1_replica = true;
    let mut partitions_have_2_replicas = true;

    for topic in metadata.topic_metadata.iter() {
        for partition in topic.partition_metadata.iter() {
            if partition.isr.is_empty() {
                println!("ERR: topic {} partition {} has no active replicas", topic.topic, partition.partition);
                partitions_have_1_replica = false;
            }
            if partition.isr.len() <= partition.replicas.len() / 2 {
                println!("WARN: topic {} partition {} is under-replicated", topic.topic, partition.partition);
                partitions_have_2_replicas = false;
            }
        }
    }

    if !partitions_have_1_replica {
        println!("ERR: Some partitions have no active replica!");
        exit(2);
    } else if !partitions_have_2_replicas {
        println!("ERR: Some partitions are under-replicated");
        exit(2);
    } else if !offline_brokers.is_empty() {
        println!("ERR: Some brokers are offline");
        exit(3);
    } else if !brokers_up_to_date {
        println!("WARN: Some brokers are still getting in sync");
        exit(3);
    } else {
        println!("Cluster is healthy");
        exit(0);
    }
}

async fn load_metadata(conn: &mut KafkaCluster) -> Result<metadata::Response> {
    conn.send_any(&metadata::Request {
        topics: None,
        allow_auto_topic_creation: false
    }).await
}