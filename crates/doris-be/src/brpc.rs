//! The BE's brpc port (8060). The FE calls `PBackendService` here as gRPC over HTTP/2. doris-rs
//! implements none of it in M2: every call answers grpc-status 0 with a result message whose
//! field 1, the `PStatus status` every `P*Result` in `internal_service.proto` leads with, is
//! NOT_IMPLEMENTED_ERROR (3) naming the method. Bytes that are not HTTP/2 (brpc's own
//! `baidu_std` protocol) end the connection.

use std::convert::Infallible;
use std::pin::Pin;
use std::task::{Context, Poll};

use ::http::{HeaderMap, HeaderValue, Request, Response, StatusCode};
use bytes::Bytes;
use doris_proto::doris::PStatus;
use hyper::body::{Body, Frame, Incoming};
use hyper::service::service_fn;
use hyper_util::rt::{TokioExecutor, TokioIo};

const NOT_IMPLEMENTED_ERROR: i32 = 3;

pub async fn serve(listener: tokio::net::TcpListener) {
    loop {
        let Ok((stream, _)) = listener.accept().await else { continue };
        tokio::spawn(async move {
            let svc = service_fn(|req: Request<Incoming>| async move { Ok::<_, Infallible>(answer(&req)) });
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

fn answer(req: &Request<Incoming>) -> Response<GrpcBody> {
    let method = req.uri().path().rsplit('/').next().unwrap_or_default();
    let mut trailers = HeaderMap::new();
    trailers.insert("grpc-status", HeaderValue::from_static("0"));
    let body = GrpcBody { data: Some(not_implemented_message(method)), trailers: Some(trailers) };
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
