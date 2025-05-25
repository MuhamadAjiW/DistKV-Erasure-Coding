use reqwest::Client;
use tracing::info;

use crate::helper::_cluster_helper::TestCluster;

#[tokio::test]
async fn test_single_node_http_healthcheck_from_config() {
    let cluster = TestCluster::new(1).await;
    let node = &cluster.nodes[0];

    let client = Client::new();

    info!("[Test] Checking healthcheck...");
    let resp = client
        .get(&format!("http://{}/", node.http_address.to_string()))
        .send()
        .await
        .expect("Failed to send healthcheck request");
    assert!(
        resp.status().is_success(),
        "Healthcheck failed: {:?}",
        resp.status()
    );
    let body = resp.text().await.expect("Failed to get healthcheck body");
    assert_eq!(body, "HTTP server running just fine");
    info!("[Test] Healthcheck passed.");
}
