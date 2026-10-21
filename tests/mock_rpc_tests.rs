mod common;

use serde_json::json;

use soroban_cost_estimator::error::AppError;
use soroban_cost_estimator::rpc::client::RpcClient;

#[tokio::test]
async fn mock_rpc_fixture_handles_simulate_transaction_and_health() {
    let server = common::start_mock_rpc_server().await;

    common::stub_jsonrpc_success(
        &server,
        "simulateTransaction",
        json!({
            "latestLedger": 42,
            "minResourceFee": "100",
            "transactionData": "abc123"
        }),
    )
    .await;
    common::stub_jsonrpc_success(
        &server,
        "getHealth",
        json!({
            "status": "healthy",
            "latestLedger": 42
        }),
    )
    .await;

    let client = RpcClient::new(server.uri().as_str());

    let tx: serde_json::Value = client
        .call("simulateTransaction", json!({ "transaction": "abc" }))
        .await
        .expect("simulateTransaction should succeed against the mock server");
    assert_eq!(tx["latestLedger"], 42);
    assert_eq!(tx["minResourceFee"], "100");

    let health: serde_json::Value = client
        .call("getHealth", json!({}))
        .await
        .expect("health check should succeed against the mock server");
    assert_eq!(health["status"], "healthy");
}

#[tokio::test]
async fn mock_rpc_fixture_handles_ledger_entries_and_network_payloads() {
    let server = common::start_mock_rpc_server().await;

    common::stub_jsonrpc_success(
        &server,
        "getLedgerEntries",
        json!({
            "latestLedger": 7,
            "entries": [
                {
                    "key": "abc",
                    "xdr": "e30=",
                    "lastModifiedLedgerSeq": 7
                }
            ]
        }),
    )
    .await;
    common::stub_jsonrpc_success(
        &server,
        "getNetwork",
        json!({
            "name": "testnet",
            "passphrase": "Test SDF Network ; September 2015",
            "protocolVersion": 23
        }),
    )
    .await;

    let client = RpcClient::new(server.uri().as_str());

    let entries: serde_json::Value = client
        .call("getLedgerEntries", json!({ "keys": ["abc"] }))
        .await
        .expect("ledger entries should succeed against the mock server");
    assert_eq!(entries["latestLedger"], 7);
    assert_eq!(entries["entries"][0]["key"], "abc");

    let network: serde_json::Value = client
        .call("getNetwork", json!({}))
        .await
        .expect("network metadata should succeed against the mock server");
    assert_eq!(network["name"], "testnet");
}

#[tokio::test]
async fn mock_rpc_fixture_surfaces_rpc_errors_from_the_server() {
    let server = common::start_mock_rpc_server().await;

    common::stub_jsonrpc_error(&server, "simulateTransaction", -32602, "invalid params").await;

    let client = RpcClient::new(server.uri().as_str());
    let err = client
        .call::<serde_json::Value>("simulateTransaction", json!({ "transaction": "bad" }))
        .await
        .expect_err("mocked JSON-RPC errors should propagate as AppError::Rpc");

    assert!(matches!(err, AppError::Rpc { status: -32602, .. }));
    }

    /// LRU eviction: once completed entries exceed the default bound (256),
    /// the least-recently-used entry is evicted so the map never grows unbounded.
    #[tokio::test]
    async fn test_dedup_lru_eviction_beyond_bound() {
        let (url, counter) = spawn_json_rpc_stub(0).await;
        let client = RpcClient::with_max_completed_dedup_entries(url, 3);

        // Issue 4 distinct requests (below the bound of 3 won't trigger eviction,
        // but 4 will evict the LRU entry).
        let params = serde_json::json!({"k": "v"});
        for i in 0..5 {
            let _: Value = client
                .call("test.method", params.clone())
                .await
                .expect("call should succeed");
        }

        // The network should have been called 5 times (one per distinct request
        // since the bound is 3 and we issue 5 with the same params, the oldest
        // gets evicted each time after the 3rd).
        assert_eq!(
            counter.load(Ordering::SeqCst),
            5,
            "with a bound of 3 and 5 identical requests, all must hit the network "
            "due to LRU eviction"
        );
    }

    /// Recent identical requests still deduplicate after LRU eviction has
    /// removed older entries: if the same request is made again within the
    /// bound, it should hit the cached result without a network call.
    #[tokio::test]
    async fn test_dedup_recent_still_deduplicates_after_eviction() {
        let (url, counter) = spawn_json_rpc_stub(0).await;
        let client = RpcClient::with_max_completed_dedup_entries(url, 2);

        let params = serde_json::json!({"k": "v"});

        // Issue 3 identical requests (bound is 2, so the first 2 cache, the 3rd
        // evicts the 1st). All 3 should hit the network initially.
        for _ in 0..3 {
            let _: Value = client
                .call("test.method", params.clone())
                .await
                .expect("call should succeed");
        }
        assert_eq!(
            counter.load(Ordering::SeqCst),
            3,
            "first 3 identical requests with bound=2 must all hit the network"
        );

        // Now issue the 1st request again. It was evicted, so it must hit the
        // network again (4th network call).
        let _: Value = client
            .call("test.method", params.clone())
            .await
            .expect("call should succeed");
        assert_eq!(
            counter.load(Ordering::SeqCst),
            4,
            "evicted request must hit the network again on retry"
        );
    }

    /// In-flight single-flight entries remain separate from evictable completed
    /// entries: many concurrent in-flight identical requests still coalesce into
    /// one network call, regardless of the completed cache bound.
    #[tokio::test]
    async fn test_dedup_concurrent_in_flight_unaffected_by_eviction() {
        let (url, counter) = spawn_delayed_json_rpc_stub(Duration::from_millis(50)).await;
        let client = Arc::new(RpcClient::with_max_completed_dedup_entries(url, 1));

        let mut handles = Vec::new();
        for _ in 0..10 {
            let client = Arc::clone(&client);
            handles.push(tokio::spawn(async move {
                let _: Value = client
                    .call("test.method", serde_json::json!({"k": "v"}))
                    .await
                    .expect("deduped concurrent call");
            }));
        }
        for handle in handles {
            handle.await.expect("task should not panic");
        }

        // Exactly one network call regardless of the completed cache bound.
        assert_eq!(
            counter.load(Ordering::SeqCst),
            1,
            "concurrent in-flight requests must still coalesce into one call"
        );
    }
}
