extern crate kafka_protocol;

use kafka_protocol::KafkaConnection;
use kafka_protocol::messages::metadata;
use std::env::args;
use std::io::Result;
use std::process::exit;

fn main() -> Result<()> {
    let addr = args().nth(1).unwrap_or("localhost:9092".into());
    let mut conn = KafkaConnection::connect(addr)?;
    let metadata = load_metadata(&mut conn)?;

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

    let mut partitions_have_1_replica = true;
    let mut partitions_have_2_replicas = true;

    for topic in metadata.topic_metadata.iter() {
        for partition in topic.partition_metadata.iter() {
            if partition.isr.len() < 1 {
                println!("ERR: topic {} partition {} has no active replicas", topic.topic, partition.partition);
                partitions_have_1_replica = false;
            }
            if partition.isr.len() < partition.replicas.len() / 2 {
                println!("WARN: topic {} partition {} is under-replicated", topic.topic, partition.partition);
                partitions_have_2_replicas = false;
            }
        }
    }

    if !partitions_have_1_replica {
        println!("ERR: Some partitions have no active replica!");
        exit(1);
    } else if !partitions_have_2_replicas {
        println!("ERR: Some partitions are under-replicated");
        exit(1);
    } else if !brokers_up_to_date {
        println!("WARN: Some brokers are still getting in sync");
        exit(2);
    } else {
        println!("Cluster is healthy");
        exit(0);
    }
}

fn load_metadata(conn: &mut KafkaConnection) -> Result<metadata::Response> {
    let metadata_req = metadata::Request {
        topics: None,
        allow_auto_topic_creation: false
    };
    conn.send(&metadata_req)
}