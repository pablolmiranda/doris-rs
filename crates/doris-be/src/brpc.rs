//! The BE's brpc port (8060). The FE calls `PBackendService` here as gRPC over HTTP/2.
//! `exec_plan_fragment` goes to the load service when its fragment writes a table
//! (OLAP_TABLE_SINK) and to the query service otherwise; `exec_plan_fragment_prepare` and
//! `_start`, `fetch_data`, `update_cache` and `fetch_cache` are the query service's, and
//! `cancel_plan_fragment` goes to both. Every other call answers grpc-status 0 with a result
//! message whose field 1, the
//! `PStatus status` every `P*Result` in `internal_service.proto` leads with, is
//! NOT_IMPLEMENTED_ERROR (3) naming the method. Bytes that are not HTTP/2 (brpc's own
//! `baidu_std` protocol) end the connection.

use std::convert::Infallible;
use std::pin::Pin;
use std::task::{Context, Poll};

use ::http::{HeaderMap, HeaderValue, Request, Response, StatusCode};
use bytes::Bytes;
use doris_exec::fragment::decode_fragments;
use doris_proto::doris::{PCancelPlanFragmentRequest, PExecPlanFragmentRequest, PExecPlanFragmentStartRequest, PFetchDataRequest, PStatus};
use http_body_util::BodyExt;
use prost::Message;

use crate::load::LoadService;
use crate::query::QueryService;
use hyper::body::{Body, Frame, Incoming};
use hyper::service::service_fn;
use hyper_util::rt::{TokioExecutor, TokioIo};

const NOT_IMPLEMENTED_ERROR: i32 = 3;

/// The services behind the brpc port (none in tests that only need the port to answer).
#[derive(Clone, Default)]
pub struct Services {
    pub load: Option<LoadService>,
    pub query: Option<QueryService>,
}

pub async fn serve(listener: tokio::net::TcpListener, services: Services) {
    loop {
        let Ok((stream, _)) = listener.accept().await else { continue };
        let services = services.clone();
        tokio::spawn(async move {
            let svc = service_fn(move |req: Request<Incoming>| {
                let services = services.clone();
                async move { Ok::<_, Infallible>(answer(req, services).await) }
            });
            let _ = hyper::server::conn::http2::Builder::new(TokioExecutor::new())
                .serve_connection(TokioIo::new(stream), svc)
                .await;
        });
    }
}

/// The gRPC response body for a method: one uncompressed message, `{1: PStatus}`.
pub fn not_implemented_message(method: &str) -> Bytes {
    let status = PStatus {
        status_code: NOT_IMPLEMENTED_ERROR,
        error_msgs: vec![format!("doris-rs does not support PBackendService.{method} yet")],
    };
    let mut msg = Vec::new();
    prost::encoding::message::encode(1, &status, &mut msg);
    let mut framed = vec![0u8];
    framed.extend_from_slice(&(msg.len() as u32).to_be_bytes());
    framed.extend_from_slice(&msg);
    Bytes::from(framed)
}

/// One gRPC message: `compressed flag | u32 BE length | message`.
fn frame(msg: &[u8]) -> Bytes {
    let mut framed = vec![0u8];
    framed.extend_from_slice(&(msg.len() as u32).to_be_bytes());
    framed.extend_from_slice(msg);
    Bytes::from(framed)
}

fn unframe(body: &[u8]) -> &[u8] {
    if body.len() >= 5 && body[0] == 0 {
        let n = u32::from_be_bytes(body[1..5].try_into().expect("4 bytes")) as usize;
        &body[5..(5 + n).min(body.len())]
    } else {
        body
    }
}

/// Whether an exec request is a write the load service runs (`INSERT … VALUES`); every other
/// request, `INSERT … SELECT` included, is the query service's.
pub fn is_write(req: &PExecPlanFragmentRequest) -> bool {
    decode_fragments(req).is_ok_and(|l| doris_exec::fragment::is_load_plan(&l))
}

async fn blocking<T: Send + 'static>(f: impl FnOnce() -> T + Send + 'static) -> Option<T> {
    tokio::task::spawn_blocking(f).await.ok()
}

async fn answer(req: Request<Incoming>, services: Services) -> Response<GrpcBody> {
    let method = req.uri().path().rsplit('/').next().unwrap_or_default().to_string();
    let body = req.into_body().collect().await.map(|b| b.to_bytes()).unwrap_or_default();
    let msg = unframe(&body).to_vec();
    let Services { load, query } = services;
    let reply: Option<Vec<u8>> = match (method.as_str(), load, query) {
        ("exec_plan_fragment", load, query) => match PExecPlanFragmentRequest::decode(msg.as_slice()) {
            Ok(r) if is_write(&r) => match load {
                Some(load) => blocking(move || load.exec_plan_fragment(&r).encode_to_vec()).await,
                None => None,
            },
            Ok(r) => match query {
                Some(query) => blocking(move || query.exec_plan_fragment(&r).encode_to_vec()).await,
                None => None,
            },
            Err(_) => None,
        },
        ("exec_plan_fragment_prepare", _, Some(query)) => match PExecPlanFragmentRequest::decode(msg.as_slice()) {
            Ok(r) => blocking(move || query.prepare(&r).encode_to_vec()).await,
            Err(_) => None,
        },
        ("exec_plan_fragment_start", _, Some(query)) => {
            let r = PExecPlanFragmentStartRequest::decode(msg.as_slice()).unwrap_or_default();
            blocking(move || query.start(&r).encode_to_vec()).await
        }
        ("fetch_data", _, Some(query)) => match PFetchDataRequest::decode(msg.as_slice()) {
            Ok(r) => blocking(move || query.fetch_data(&r).encode_to_vec()).await,
            Err(_) => None,
        },
        ("cancel_plan_fragment", load, query) if load.is_some() || query.is_some() => {
            let r = PCancelPlanFragmentRequest::decode(msg.as_slice()).unwrap_or_default();
            let mut out = None;
            if let Some(load) = load {
                out = Some(load.cancel_plan_fragment(&r).encode_to_vec());
            }
            if let Some(query) = query {
                out = Some(query.cancel(&r).encode_to_vec());
            }
            out
        }
        ("update_cache", _, Some(query)) => Some(query.update_cache().encode_to_vec()),
        ("fetch_cache", _, Some(query)) => Some(query.fetch_cache().encode_to_vec()),
        _ => None,
    };
    let data = reply.map(|r| frame(&r)).unwrap_or_else(|| not_implemented_message(&method));
    let mut trailers = HeaderMap::new();
    trailers.insert("grpc-status", HeaderValue::from_static("0"));
    let body = GrpcBody { data: Some(data), trailers: Some(trailers) };
    Response::builder().status(StatusCode::OK).header("content-type", "application/grpc").body(body).expect("valid response")
}

/// One data frame, then the trailers.
pub struct GrpcBody {
    data: Option<Bytes>,
    trailers: Option<HeaderMap>,
}

impl Body for GrpcBody {
    type Data = Bytes;
    type Error = Infallible;

    fn poll_frame(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Result<Frame<Bytes>, Infallible>>> {
        if let Some(data) = self.data.take() {
            return Poll::Ready(Some(Ok(Frame::data(data))));
        }
        Poll::Ready(self.trailers.take().map(|t| Ok(Frame::trailers(t))))
    }
}
