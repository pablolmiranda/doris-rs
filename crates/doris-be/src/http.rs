//! The BE's HTTP port (webserver_port, 8040), as the C++ BE's `EvHttpServer` answers it. Served
//! as the C++ BE serves them: `/api/health` (`HealthAction`),
//! `/api/running_pipeline_tasks/{duration}` (`LongPipelineTaskAction`), stream load
//! (`PUT /api/{db}/{table}/_stream_load`, `StreamLoadAction`) and its error logs
//! (`/api/_load_error_log`). Every other route the C++ BE registers answers like a C++ action
//! that failed, `Status::to_json` with a message naming the route; a path no route matches gets
//! the C++ BE's 404.

use std::collections::BTreeMap;
use std::convert::Infallible;
use std::net::SocketAddr;

use ::http::{header, Request, Response, StatusCode};
use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use hyper::service::service_fn;
use hyper_util::rt::{TokioExecutor, TokioIo};
use hyper_util::server::conn::auto;

use crate::load::{LoadService, StreamLoadRequest};

/// BE route templates (`be/src/service/http_service.cpp`), `{x}` matching one segment.
const ROUTES: &str = include_str!("../../../oracle/wire/be_http_routes.txt");

/// Serves until the listener fails.
pub async fn serve(listener: tokio::net::TcpListener, load: Option<LoadService>) {
    loop {
        let Ok((stream, peer)) = listener.accept().await else { continue };
        let load = load.clone();
        tokio::spawn(async move {
            let svc = service_fn(move |req: Request<Incoming>| {
                let load = load.clone();
                async move { Ok::<_, Infallible>(answer(req, peer, load).await) }
            });
            let _ = auto::Builder::new(TokioExecutor::new()).serve_connection(TokioIo::new(stream), svc).await;
        });
    }
}

fn reply(status: StatusCode, content_type: &str, body: String) -> Response<Full<Bytes>> {
    Response::builder()
        .status(status)
        .header(header::CONTENT_TYPE, content_type)
        .body(Full::new(Bytes::from(body)))
        .expect("valid response")
}

/// The route template a path matches.
fn route(path: &str) -> Option<&'static str> {
    let segs: Vec<&str> = path.trim_end_matches('/').split('/').collect();
    ROUTES.lines().filter(|l| l.starts_with('/')).map(str::trim).find(|r| {
        let rs: Vec<&str> = r.split('/').collect();
        rs.len() == segs.len() && rs.iter().zip(&segs).all(|(r, s)| (r.starts_with('{') && r.ends_with('}') && !s.is_empty()) || r == s)
    })
}

async fn answer(req: Request<Incoming>, peer: SocketAddr, load: Option<LoadService>) -> Response<Full<Bytes>> {
    let path = req.uri().path().to_string();
    match (route(&path), load) {
        (Some("/api/{db}/{table}/_stream_load"), Some(load)) if req.method() == ::http::Method::PUT => {
            return stream_load(req, &path, peer, load).await;
        }
        (Some("/api/_load_error_log"), Some(load)) => {
            let file = req.uri().query().unwrap_or_default().split('&').find_map(|kv| kv.strip_prefix("file=")).unwrap_or_default().to_string();
            return match load.error_log(&file) {
                Some(b) => reply(StatusCode::OK, "text/plain; charset=utf-8", String::from_utf8_lossy(&b).into_owned()),
                None => reply(StatusCode::NOT_FOUND, "text/html; charset=ISO-8859-1", "Not Found".into()),
            };
        }
        _ => {}
    }
    answer_static(&path)
}

/// `StreamLoadAction`: the body is read whole (hyper sends `100 Continue` when it is first
/// read), the load runs off the async threads, and the reply is always 200 with the JSON.
async fn stream_load(req: Request<Incoming>, path: &str, peer: SocketAddr, load: LoadService) -> Response<Full<Bytes>> {
    let parts: Vec<&str> = path.split('/').collect();
    let headers: BTreeMap<String, String> = req
        .headers()
        .iter()
        .map(|(k, v)| (k.as_str().to_ascii_lowercase(), String::from_utf8_lossy(v.as_bytes()).into_owned()))
        .collect();
    let body = req.into_body().collect().await.map(|b| b.to_bytes().to_vec()).unwrap_or_default();
    let request = StreamLoadRequest {
        db: parts.get(2).copied().unwrap_or_default().to_string(),
        table: parts.get(3).copied().unwrap_or_default().to_string(),
        headers,
        body,
        client_ip: peer.ip().to_string(),
    };
    let json = tokio::task::spawn_blocking(move || load.stream_load(request)).await.unwrap_or_default();
    reply(StatusCode::OK, "text/html; charset=ISO-8859-1", json)
}

fn answer_static(path: &str) -> Response<Full<Bytes>> {
    match route(path) {
        Some("/api/health") => reply(StatusCode::OK, "application/json", r#"{"status": "OK","msg": "OK"}"#.into()),
        Some("/api/running_pipeline_tasks/{duration}") => {
            let duration = path.rsplit('/').next().unwrap_or_default();
            let ctype = "text/plain; version=0.0.4";
            match duration.parse::<i64>() {
                Ok(d) => reply(StatusCode::OK, ctype, format!("0 pipeline fragment contexts are still running! duration_limit={d}\n")),
                Err(_) => reply(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    ctype,
                    format!("invalid argument.duration: {duration}, meet error: stoll"),
                ),
            }
        }
        Some(r) => reply(StatusCode::OK, "application/json", status_json("NOT_IMPLEMENTED_ERROR", &format!("doris-rs does not support {r} yet"))),
        None => reply(StatusCode::NOT_FOUND, "text/html; charset=ISO-8859-1", "Not Found".into()),
    }
}

/// `Status::to_json`: rapidjson's PrettyWriter, four-space indent.
fn status_json(code: &str, msg: &str) -> String {
    let esc = |s: &str| s.replace('\\', "\\\\").replace('"', "\\\"");
    format!("{{\n    \"status\": \"{}\",\n    \"msg\": \"{}\"\n}}", esc(code), esc(msg))
}
