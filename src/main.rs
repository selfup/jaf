pub mod merge;

use merge::merge_and_inject;

use bytes::Bytes;
use futures::TryStreamExt;
use hyper::{
    Client, Request, Response, Server, Uri,
    body::Body,
    client::HttpConnector,
    header::CONTENT_TYPE,
    service::{make_service_fn, service_fn},
};
use serde_json::{Value, json};
use std::convert::Infallible;
use tokio_util::io::StreamReader;

const JAF_ENTRY_POINT: &'static str = "127.0.0.1:3000";
const JAF_PROXY_LISTENING_ON: &'static str = "JAF listening on";
const JAF_UPSTREAM_ERROR: &'static str = "JAF -- Upstream error";
const JAF_PROXY_FLAG: &'static str = "JAF Proxied";
const APPLICATION_JSON: &'static str = "application/json";
const CONTENT_LENGTH: &'static str = "content-length";

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let addr: core::net::SocketAddr = ([127, 0, 0, 1], 3000).into();
    let client: Client<HttpConnector> = Client::new();

    let make_svc = make_service_fn(move |_| {
        let client: hyper::client::Client<HttpConnector> = client.clone();

        async move {
            Ok::<_, Infallible>(service_fn(move |req: Request<Body>| {
                proxy_handler(req, client.clone())
            }))
        }
    });

    println!("{} http://{}", JAF_PROXY_LISTENING_ON, addr);

    Server::bind(&addr).serve(make_svc).await?;

    Ok(())
}

async fn proxy_handler(
    mut req: Request<Body>,
    client: Client<HttpConnector>,
) -> Result<Response<Body>, Infallible> {
    if let Some(auth) = req.uri().authority().map(|a| a.as_str()) {
        if auth == JAF_ENTRY_POINT {
            let mut parts = req.uri().clone().into_parts();

            parts.scheme = Some("https".parse().unwrap());
            parts.authority = Some("api.example.com".parse().unwrap());

            *req.uri_mut() = Uri::from_parts(parts).unwrap();
        }
    }

    if let Some(content_type) = req.headers().get(CONTENT_TYPE) {
        if content_type
            .to_str()
            .unwrap_or_default()
            .starts_with(APPLICATION_JSON)
        {
            let method: hyper::Method = req.method().clone();
            let uri: Uri = req.uri().clone();
            let headers: hyper::HeaderMap = req.headers().clone();

            let (mut sender, new_body) = Body::channel();
            let body_stream = req
                .into_body()
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e));

            tokio::spawn(async move {
                let merged = merge_and_inject(StreamReader::new(body_stream))
                    .await
                    .unwrap();

                let bytes: Vec<u8> = serde_json::to_vec(&merged).unwrap();

                sender.send_data(Bytes::from(bytes)).await.unwrap();
                sender.send_trailers(Default::default()).await.unwrap();
            });

            let mut builder = Request::builder().method(method).uri(uri);

            for (k, v) in headers.iter() {
                builder = builder.header(k, v);
            }

            req = builder.body(new_body).unwrap();
        }
    }

    let resp = match client.request(req).await {
        Ok(r) => r,
        Err(e) => {
            return Ok(Response::builder()
                .status(502)
                .body(Body::from(format!("{}: {}", JAF_UPSTREAM_ERROR, e)))
                .unwrap());
        }
    };

    if let Some(content_type) = resp.headers().get(hyper::header::CONTENT_TYPE) {
        if content_type
            .to_str()
            .unwrap_or_default()
            .starts_with(APPLICATION_JSON)
        {
            let status: hyper::StatusCode = resp.status();
            let mut headers: hyper::HeaderMap = resp.headers().clone();

            headers.remove(CONTENT_TYPE);
            headers.remove(CONTENT_LENGTH);

            let (mut sender, body) = Body::channel();

            let body_stream = resp
                .into_body()
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e));

            tokio::spawn(async move {
                let mut map: serde_json::map::Map<String, Value> =
                    match merge_and_inject(StreamReader::new(body_stream))
                        .await
                        .unwrap()
                    {
                        Value::Object(map) => map,
                        other => {
                            let mut serde_map: serde_json::map::Map<String, Value> =
                                serde_json::Map::new();

                            serde_map.insert("value".into(), other);

                            serde_map
                        }
                    };

                map.insert(JAF_PROXY_FLAG.to_string(), json!(true));

                let bytes: Vec<u8> = serde_json::to_vec(&Value::Object(map)).unwrap();

                sender.send_data(Bytes::from(bytes)).await.unwrap();
                sender.send_trailers(Default::default()).await.unwrap();
            });

            let mut builder: hyper::http::response::Builder = Response::builder().status(status);

            for (k, v) in headers.iter() {
                builder = builder.header(k, v.clone());
            }

            return Ok(builder.body(body).unwrap());
        }
    }

    Ok(resp)
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use futures::stream;
    use hyper::{Body, Client, Request, StatusCode, body::to_bytes};
    use std::io;
    use tokio::sync::oneshot;
    use warp::Filter;

    #[tokio::test]
    async fn test_upstream_unreachable() {
        let uri: Uri = "http://127.0.0.1:59999/".parse().unwrap();

        let req: Request<Body> = Request::builder().uri(uri).body(Body::empty()).unwrap();
        let client: Client<HttpConnector> = Client::new();
        let resp: Response<Body> = proxy_handler(req, client).await.unwrap();

        assert_eq!(resp.status(), StatusCode::BAD_GATEWAY);

        let body: Bytes = to_bytes(resp.into_body()).await.unwrap();
        let s = std::str::from_utf8(&body).unwrap();

        assert!(s.contains(JAF_UPSTREAM_ERROR));
    }

    #[tokio::test]
    async fn test_passthrough_non_json_response() {
        let (tx, rx) = oneshot::channel();

        let server: tokio::task::JoinHandle<()> = tokio::spawn(async move {
            let route =
                warp::any().map(|| Response::builder().status(200).body("plain text").unwrap());

            let (addr, fut) = warp::serve(route).bind_ephemeral(([127, 0, 0, 1], 0));

            tx.send(addr).unwrap();

            fut.await;
        });

        let addr: std::net::SocketAddr = rx.await.unwrap();
        let uri: Uri = format!("http://{}/", addr).parse().unwrap();
        let req: Request<Body> = Request::builder().uri(uri).body(Body::empty()).unwrap();
        let client: Client<HttpConnector> = Client::new();
        let resp: Response<Body> = proxy_handler(req, client).await.unwrap();

        assert_eq!(resp.status(), 200);

        let body: Bytes = to_bytes(resp.into_body()).await.unwrap();

        assert_eq!(&body[..], b"plain text");

        server.abort();
    }

    #[tokio::test]
    async fn integration_proxy_injects() {
        let (tx, rx) = oneshot::channel();

        let server: tokio::task::JoinHandle<()> = tokio::spawn(async move {
            let route = warp::any().map(|| {
                let s = stream::iter(vec![
                    Ok::<Bytes, io::Error>(Bytes::from_static(b"{\"x\":42}")),
                    Ok(Bytes::from_static(b"{\"y\":99}")),
                ]);

                Response::builder()
                    .header(CONTENT_TYPE, APPLICATION_JSON)
                    .body(Body::wrap_stream(s))
                    .unwrap()
            });

            let (addr, fut) = warp::serve(route).bind_ephemeral(([127, 0, 0, 1], 0));

            tx.send(addr).unwrap();

            fut.await;
        });

        let addr: std::net::SocketAddr = rx.await.unwrap();
        let uri: Uri = format!("http://{}/", addr).parse().unwrap();
        let req: Request<Body> = Request::builder()
            .uri(uri)
            .header(CONTENT_TYPE, APPLICATION_JSON)
            .body(Body::empty())
            .unwrap();

        let client: Client<HttpConnector> = Client::new();
        let resp: Response<Body> = proxy_handler(req, client).await.unwrap();

        let out: Value =
            serde_json::from_slice(&to_bytes(resp.into_body()).await.unwrap()).unwrap();

        assert_eq!(out["x"], 42);
        assert_eq!(out["y"], 99);
        assert_eq!(out[JAF_PROXY_FLAG], true);

        server.abort();
    }
}
