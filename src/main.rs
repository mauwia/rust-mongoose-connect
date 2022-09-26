use chrono::{Duration as chronoDuration, Local, Utc};
use futures::StreamExt;
use mongodb::options::AggregateOptions;
use mongodb::{
    bson::doc, bson::Bson, options::ClientOptions, options::FindOptions, Client, Collection,
};
use std::time::Duration;
/**
     Example of using the mongodb driver from rust.
**/

#[tokio::main]
pub async fn main() -> mongodb::error::Result<()> {
    let dt = (Local::now() - chronoDuration::hours(166)).timestamp()*1000;

    // Parse your connection string into an options struct
    let connect_str = "";

    let mut client_options = ClientOptions::parse(connect_str).await?;
    // Manually set an option
    let duration: Duration = Duration::new(60, 0);
    client_options.app_name = Some("Rust Demo".to_string());
    client_options.connect_timeout = Some(duration);
    // Get a handle to the cluster
    let client = Client::with_options(client_options)?;
    // Ping the server to see if you can connect to the cluster
    client
        .database("admin")
        .run_command(doc! {"ping": 1}, None)
        .await?;

    println!("Connected successfully. ");
    let mainnet_v2 = client
        .database("DB")
        .collection("mainnets");

    // Lets pick Upper West Side as an example
    let pipeline = [
        doc! {"$match": {"timestamp": {"$gte": dt.to_string()}}},
        doc! {"$project":{
            "hour":{
                "$hour": {
                    "date":{"$toDate":{"$toLong":"$timestamp"}},
                    "timezone":"+0500"
                }
            },
            "currentAccumalatedFeeWeight": 1,
            "networkDemand": 1,
            "timestamp": 1,
            "utcTimestamp": 1,
            "currentAccumulatedWeight": 1,
        }},
    ];
    println!("{:?}", pipeline);
    let options = AggregateOptions::default();
    let mut aggregate = mainnet_v2.aggregate(pipeline, options).await?;

    // println!("Aggregation result: {:?}", aggregate.next().await);
    // aggregate query only returns one document but the aggregation returns a AggregateCursor
    while let Some(doc) = aggregate.next().await {
        match doc {
            Ok(document) => {
                // println!("OK");
                // let coordinates = document.get("coord").unwrap();
                println!("{}", document);
                // query_restaurants(&restaurants, coordinates).await;
            }
            Err(_err) => println!("error: "),
        }
    }

    Ok(())
}

