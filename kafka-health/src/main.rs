extern crate serde;
extern crate serde_json;
extern crate zookeeper;

use serde::Deserialize;
use std::rc::Rc;
use std::collections::HashMap;
use std::process::exit;
use zookeeper::{ZooKeeper, ZkResult};

fn main() {
    let zk = connect().unwrap();

    let mut brokers = zk.get_children("/brokers/ids", false).unwrap();
    brokers.sort();

    let brokers: Vec<Broker> = brokers.into_iter()
        .map(|id| {
            let info = load_broker_info(&id, &zk);
            Broker {
                id: id.parse().unwrap(),
                info
            }
        })
        .collect();

    let mut topics = zk.get_children("/brokers/topics", false).unwrap();
    topics.sort();

    let partitions: Vec<Partition> = topics.into_iter()
        .flat_map(|name| {
            let name = Rc::new(name);
            let data = load_topic_info(&name, &zk);
            data.partitions.into_iter()
                .map(move |(part, replicas)| (name.clone(), part.parse().unwrap(), replicas))
        })
        .map(|(name, part, replicas)| {
            let data = load_partition_info(&name, part, &zk);
            Partition {
                topic: name,
                id: part,
                replicas,
                isr: data.isr
            }
        })
        .collect();
        
    let mut brokers_up_to_date = true;

    for broker in brokers {
        let mut broker_up_to_date = true;
        println!("Broker {}:{}", broker.info.host, broker.info.port);
        for partition in partitions.iter() {
            if !partition.replicas.contains(&broker.id) {
                continue;
            }

            if !partition.isr.contains(&broker.id) {
                println!("  WARN: Catching up on topic {} partition {}", partition.topic, partition.id);
                broker_up_to_date = false;
            }
        }

        if broker_up_to_date {
            println!(" IN SYNC!");
        }

        brokers_up_to_date = brokers_up_to_date && broker_up_to_date;
    }

    let mut partitions_have_1_replica = true;
    let mut partitions_have_2_replicas = true;

    for partition in partitions {
        if partition.isr.len() < 1 {
            println!("ERR: topic {} partition {} has no active replicas", partition.topic, partition.id);
            partitions_have_1_replica = false;
        }
        if partition.isr.len() < 2 {
            println!("WARN: topic {} partition {} is under-replicated", partition.topic, partition.id);
            partitions_have_2_replicas = false;
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

fn load_broker_info(id: &str, zk: &ZooKeeper) -> BrokerInfo {
    let path = format!("/brokers/ids/{}", id);
    let (bytes, _) = zk.get_data(&path, false).unwrap();
    serde_json::from_slice(&bytes).unwrap()
}

fn load_topic_info(name: &str, zk: &ZooKeeper) -> TopicInfo {
    let path = format!("/brokers/topics/{}", name);
    let (bytes, _) = zk.get_data(&path, false).unwrap();
    serde_json::from_slice(&bytes).unwrap()
}

fn load_partition_info(topic_name: &str, partition: u8, zk: &ZooKeeper) -> PartitionInfo {
    let path = format!("/brokers/topics/{}/partitions/{}/state", topic_name, partition);
    let (bytes, _) = zk.get_data(&path, false).unwrap();
    serde_json::from_slice(&bytes).unwrap()
}

fn connect() -> ZkResult<ZooKeeper> {
    ZooKeeper::connect("localhost:2181", std::time::Duration::from_secs(30), Watcher)
}

struct Watcher;

impl zookeeper::Watcher for Watcher {
    fn handle(&self, _: zookeeper::WatchedEvent) {
        // ignore
    }
}

#[derive(Debug)]
struct Broker {
    id: u8,
    info: BrokerInfo
}

#[derive(Debug,Deserialize)]
struct BrokerInfo {
    host: String,
    port: u16
}

#[derive(Debug)]
struct Partition {
    topic: Rc<String>,
    id: u8,
    replicas: Vec<u8>,
    isr: Vec<u8>
}

#[derive(Debug,Deserialize)]
struct TopicInfo {
    partitions: HashMap<String, Vec<u8>>
}

#[derive(Debug,Deserialize)]
struct PartitionInfo {
    isr: Vec<u8>
}