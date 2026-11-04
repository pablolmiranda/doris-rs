//! The BE's HTTP port (webserver_port, 8040), as the C++ BE's `EvHttpServer` answers it. Two
//! routes the FE and the harness call on a clock are served as the C++ BE serves them:
//! `/api/health` (`HealthAction`) and `/api/running_pipeline_tasks/{duration}`
//! (`LongPipelineTaskAction`). Every other route the C++ BE registers answers like a C++ action
//! that failed, `Status::to_json` with a message naming the route; a path no route matches gets
//! the C++ BE's 404.

use std::convert::Infallible;

use ::http::{header, Request, Response, StatusCode};
use bytes::Bytes;
use http_body_util::Full;
use hyper::body::Incoming;
use hyper::service::service_fn;
use hyper_util::rt::{TokioExecutor, TokioIo};
use hyper_util::server::conn::auto;

/// BE route templates (`be/src/service/http_service.cpp`), `{x}` matching one segment.
const ROUTES: &str = include_str!("../../../oracle/wire/be_http_routes.txt");

/// Serves until the listener fails.
pub async fn serve(listener: tokio::net::TcpListener) {
    loop {
        let Ok((stream, _)) = listener.accept().await else { continue };
        tokio::spawn(async move {
            let svc = service_fn(|req: Request<Incoming>| async move { Ok::<_, Infallible>(answer(&req)) });
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

fn answer(req: &Request<Incoming>) -> Response<Full<Bytes>> {
    let path = req.uri().path();
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
