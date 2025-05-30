use std::sync::Arc;

use anyhow::Result;
use bytes::Bytes;
use dashmap::DashMap;
use futures::future::join_all;
use rand::Rng;
use tokio::join;

struct Channel {
    original: Bytes,
    clients: Vec<Bytes>,
}

struct DecodedChannel {
    original: String,
    clients: Vec<String>,
}

impl DecodedChannel {
    fn from_channel(channel: &Channel) -> Result<Self> {
        Ok(DecodedChannel {
            original: String::from_utf8(channel.original.to_vec())?,
            clients: channel
                .clients
                .iter()
                .map(|c| String::from_utf8(c.to_vec()).map_err(Into::into))
                .collect::<Result<Vec<_>>>()?,
        })
    }
}

const CHANNEL_COUNT: usize = 2000;

#[tokio::main]
async fn main() {
    let map: Arc<DashMap<String, Channel>> = Arc::new(DashMap::new());
    let client = reqwest::Client::builder()
        .http2_prior_knowledge()
        .build()
        .unwrap();
    let handles = (0..CHANNEL_COUNT)
        .map(|o| {
            let channel_name = Arc::new(format!("channel_{}", o));
            let freq = rand::rng().random_range(500.0f32..=1000.0).round();
            let max = 2000;
            let client_count = rand::rng().random_range(5..=10);
            let channel = Channel {
                original: Bytes::new(),
                clients: vec![Bytes::new(); client_count],
            };
            map.insert(channel_name.to_string(), channel);
            tokio::spawn({
                let map = map.clone();
                let channel_name = channel_name.clone();
                let client = client.clone();
                async move {
                    let response = match client
                        .post(format!("http://localhost:3000?freq={}&max={}", freq, max))
                        .body(channel_name.to_string())
                        .send()
                        .await
                    {
                        Ok(response) => response,
                        Err(e) => {
                            eprintln!("Channel not found: {}, becausee {:?}", &channel_name, e);
                            return;
                        }
                    };

                    let client_handles = (0..client_count).map(|j| {
                        let channel_name = channel_name.clone();
                        let map = map.clone();
                        let client = client.clone();
                        let delay = rand::rng().random_range(0..=1000);
                        tokio::spawn(async move {
                            tokio::time::sleep(std::time::Duration::from_millis(delay)).await;
                            let response = match client
                                .get(format!("http://localhost:3000?id={}", &channel_name))
                                .send()
                                .await
                            {
                                Ok(response) => response,
                                Err(e) => {
                                    eprintln!(
                                        "Channel not found: {}, becausee {:?}",
                                        &channel_name, e
                                    );
                                    return;
                                }
                            };
                            map.get_mut(channel_name.as_ref()).unwrap().clients[j] =
                                match response.bytes().await {
                                    Ok(bytes) => bytes,
                                    Err(e) => {
                                        eprintln!(
                                            "Failed to get bytes for channel {}: {:?}",
                                            &channel_name, e
                                        );
                                        return;
                                    }
                                };
                        })
                    });

                    if let (Err(e), _) = join!(
                        tokio::spawn({
                            let channel_name = channel_name.clone();
                            let map = map.clone();
                            async move {
                                map.get_mut(channel_name.as_ref()).unwrap().original =
                                    match response.bytes().await {
                                        Ok(bytes) => bytes,
                                        Err(e) => {
                                            eprintln!(
                                                "Failed to get bytes for channel {}: {:?}",
                                                &channel_name, e
                                            );
                                            return;
                                        }
                                    };
                            }
                        }),
                        join_all(client_handles)
                    ) {
                        eprintln!("Error occurred while joining tasks: {:?}", e);
                    }
                }
            })
        })
        .collect::<Vec<_>>();
    join_all(handles).await;

    // Check if for each Channel in map all clients match the original
    for entry in map.iter() {
        let channel = entry.value();
        let decoded = match DecodedChannel::from_channel(channel) {
            Ok(decoded) => decoded,
            Err(e) => {
                eprintln!("Failed to decode channel {}: {:?}", entry.key(), e);
                continue;
            }
        };
        let all_match = decoded.clients.iter().all(|c| c == &decoded.original);
        if !all_match {
            eprintln!(
                "Not all clients match the original for channel {}",
                entry.key()
            );
        }
    }
}
