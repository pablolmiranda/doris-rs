use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::time::Duration;

use bytes::Bytes;
use doris_proto::doris::PFetchDataResult;
use http_body_util::{BodyExt, Full};
use hyper_util::rt::{TokioExecutor, TokioIo};
use prost::Message;

/// Starts the HTTP and brpc listeners on free ports and returns their addresses.
fn start() -> (tokio::runtime::Runtime, SocketAddr, SocketAddr) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let (http, brpc) = rt.block_on(async {
        let http = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let brpc = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addrs = (http.local_addr().unwrap(), brpc.local_addr().unwrap());
        tokio::spawn(doris_be::http::serve(http));
        tokio::spawn(doris_be::brpc::serve(brpc));
        addrs
    });
    (rt, http, brpc)
}

/// One HTTP/1.1 GET: (status, Content-Type, body).
fn get(addr: SocketAddr, path: &str) -> (u16, String, String) {
    let mut s = TcpStream::connect(addr).unwrap();
    s.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    write!(s, "GET {path} HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n").unwrap();
    let mut out = String::new();
    s.read_to_string(&mut out).unwrap();
    let (head, body) = out.split_once("\r\n\r\n").unwrap();
    let status = head.split(' ').nth(1).unwrap().parse().unwrap();
    let ctype = head
        .lines()
        .find_map(|l| l.split_once(": ").filter(|(k, _)| k.eq_ignore_ascii_case("content-type")).map(|(_, v)| v.to_string()))
        .unwrap_or_default();
    (status, ctype, body.to_string())
}

#[test]
fn health_matches_the_reference() {
    // HealthAction::handle
    let (_rt, http, _) = start();
    assert_eq!(get(http, "/api/health"), (200, "application/json".into(), r#"{"status": "OK","msg": "OK"}"#.into()));
}

#[test]
fn running_pipeline_tasks_matches_the_reference() {
    // LongPipelineTaskAction::handle with no fragments (FragmentMgr::dump_pipeline_tasks)
    let (_rt, http, _) = start();
    assert_eq!(
        get(http, "/api/running_pipeline_tasks/10"),
        (200, "text/plain; version=0.0.4".into(), "0 pipeline fragment contexts are still running! duration_limit=10\n".into())
    );
    let (status, _, body) = get(http, "/api/running_pipeline_tasks/abc");
    assert_eq!(status, 500);
    assert!(body.starts_with("invalid argument.duration: abc"), "{body}");
}

#[test]
fn an_unsupported_route_names_itself() {
    // Status::to_json, as the C++ actions answer a failed request
    let (_rt, http, _) = start();
    let (status, ctype, body) = get(http, "/api/compaction/run?tablet_id=1");
    assert_eq!((status, ctype.as_str()), (200, "application/json"));
    assert_eq!(
        body,
        "{\n    \"status\": \"NOT_IMPLEMENTED_ERROR\",\n    \"msg\": \"doris-rs does not support /api/compaction/run yet\"\n}"
    );
    let (status, _, body) = get(http, "/api/meta/header/123");
    assert_eq!(status, 200);
    assert!(body.contains("/api/meta/{op}/{tablet_id}"), "{body}");
    // EvHttpServer::on_header: no handler
    assert_eq!(get(http, "/api/no/such/route").0, 404);
    assert_eq!(get(http, "/api/no/such/route").2, "Not Found");
}

async fn grpc_call(addr: SocketAddr, method: &str) -> (http::StatusCode, Vec<u8>, Option<String>) {
    let stream = tokio::net::TcpStream::connect(addr).await.unwrap();
    let (mut sender, conn) = hyper::client::conn::http2::handshake(TokioExecutor::new(), TokioIo::new(stream)).await.unwrap();
    tokio::spawn(conn);
    // an empty request message, gRPC framed: no compression, length 0
    let body = Full::new(Bytes::from_static(&[0, 0, 0, 0, 0]));
    let req = http::Request::post(format!("http://{addr}/doris.PBackendService/{method}"))
        .header("content-type", "application/grpc")
        .header("te", "trailers")
        .body(body)
        .unwrap();
    let resp = sender.send_request(req).await.unwrap();
    let status = resp.status();
    let collected = resp.into_body().collect().await.unwrap();
    let grpc_status = collected.trailers().and_then(|t| t.get("grpc-status")).map(|v| v.to_str().unwrap().to_string());
    (status, collected.to_bytes().to_vec(), grpc_status)
}

#[test]
fn grpc_calls_get_a_not_implemented_status() {
    let (rt, _, brpc) = start();
    let (status, body, grpc_status) = rt.block_on(grpc_call(brpc, "fetch_data"));
    assert_eq!(status, 200);
    assert_eq!(grpc_status.as_deref(), Some("0"));
    assert_eq!(body[0], 0);
    let len = u32::from_be_bytes(body[1..5].try_into().unwrap()) as usize;
    let result = PFetchDataResult::decode(&body[5..5 + len]).unwrap();
    assert_eq!(result.status.status_code, 3);
    assert_eq!(result.status.error_msgs, vec!["doris-rs does not support PBackendService.fetch_data yet".to_string()]);
}

#[test]
fn baidu_std_bytes_do_not_crash() {
    let (rt, _, brpc) = start();
    let mut s = TcpStream::connect(brpc).unwrap();
    s.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    s.write_all(b"PRPC\x00\x00\x00\x10\x00\x00\x00\x08garbagegarbage!!").unwrap();
    let mut buf = [0u8; 64];
    let _ = s.read(&mut buf); // closed or an HTTP/2 GOAWAY, either is fine
    let (status, _, _) = rt.block_on(grpc_call(brpc, "exec_plan_fragment"));
    assert_eq!(status, 200);
}
