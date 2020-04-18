use clap::*;
use std::error::Error;
use std::io::Result;
use std::process::exit;

mod health;
mod lag;
mod protocol;

use protocol::KafkaCluster;

fn main() -> Result<()> {
    let matches = App::new("kafka-tools")
        .version("1.0")
        .author("Joe Frikker <jfrikker@gmail.com>")
        .about("Useful utilities for monitoring and understanding Kafka clusters")
        .setting(AppSettings::SubcommandRequired)
        .arg(Arg::with_name("server")
            .short("s")
            .long("server")
            .value_name("host:port")
            .default_value("localhost:9092")
            .multiple(true)
            .number_of_values(1)
            .help("The bootstrap host:port to connect to")
            .takes_value(true))
        .subcommand(SubCommand::with_name("health")
            .about("Verifies overall cluster health"))
        .subcommand(SubCommand::with_name("lag")
            .about("Prints a summary of consumer group lags"))
        .get_matches();

    let bootstrap = matches.values_of("server").unwrap();
    let mut cluster = match KafkaCluster::connect(bootstrap) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Error connecting to cluster: {}", e);
            exit(1);
        }
    };

    if !cluster.is_completely_connected() {
        println!("WARN: Unable to connect to all brokers");
    }

    if matches.subcommand_matches("health").is_some() {
        health::cluster_health(&mut cluster)?;
    } else if matches.subcommand_matches("lag").is_some() {
        lag::list_lags(&mut cluster)?;
    }

    Ok(())
}