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

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let addr = ([127, 0, 0, 1], 3000).into();
    let client: Client<HttpConnector> = Client::new();

    let make_svc = make_service_fn(move |_| {
        let client = client.clone();
        async move { Ok::<_, Infallible>(service_fn(move |req| proxy_handler(req, client.clone()))) }
    });

    println!("Proxy listening on http://{}", addr);

    Server::bind(&addr).serve(make_svc).await?;

    Ok(())
}

async fn proxy_handler(
    mut req: Request<Body>,
    client: Client<HttpConnector>,
) -> Result<Response<Body>, Infallible> {
    if let Some(auth) = req.uri().authority().map(|a| a.as_str()) {
        if auth == "127.0.0.1:3000" {
            let mut parts = req.uri().clone().into_parts();

            parts.scheme = Some("https".parse().unwrap());
            parts.authority = Some("api.example.com".parse().unwrap());

            *req.uri_mut() = Uri::from_parts(parts).unwrap();
        }
    }

    if let Some(ct) = req.headers().get(CONTENT_TYPE) {
        if ct
            .to_str()
            .unwrap_or_default()
            .starts_with("application/json")
        {
            let method = req.method().clone();
            let uri = req.uri().clone();
            let headers = req.headers().clone();

            let (mut sender, new_body) = Body::channel();
            let body_stream = req
                .into_body()
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e));

            tokio::spawn(async move {
                let merged = merge_and_inject(StreamReader::new(body_stream))
                    .await
                    .unwrap();

                let bytes = serde_json::to_vec(&merged).unwrap();

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
                .body(Body::from(format!("Upstream error: {}", e)))
                .unwrap());
        }
    };

    if let Some(ct) = resp.headers().get(CONTENT_TYPE) {
        if ct
            .to_str()
            .unwrap_or_default()
            .starts_with("application/json")
        {
            let status = resp.status();
            let mut headers = resp.headers().clone();

            headers.remove(CONTENT_TYPE);
            headers.remove("content-length");

            let (mut sender, body) = Body::channel();

            let body_stream = resp
                .into_body()
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e));

            tokio::spawn(async move {
                // Merge and inject proxied flag
                let mut map = match merge_and_inject(StreamReader::new(body_stream))
                    .await
                    .unwrap()
                {
                    Value::Object(map) => map,
                    other => {
                        let mut m = serde_json::Map::new();
                        m.insert("value".into(), other);
                        m
                    }
                };

                map.insert("proxied".to_string(), json!(true));

                let bytes = serde_json::to_vec(&Value::Object(map)).unwrap();

                sender.send_data(Bytes::from(bytes)).await.unwrap();
                sender.send_trailers(Default::default()).await.unwrap();
            });

            let mut builder = Response::builder().status(status);

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
    use hyper::{Body, Client, Request, StatusCode, body::to_bytes, header::CONTENT_TYPE};
    use std::io;
    use tokio::sync::oneshot;
    use warp::Filter;

    #[tokio::test]
    async fn test_upstream_unreachable() {
        let uri: Uri = "http://127.0.0.1:59999/".parse().unwrap();

        let req = Request::builder().uri(uri).body(Body::empty()).unwrap();
        let client = Client::new();
        let resp = proxy_handler(req, client).await.unwrap();

        assert_eq!(resp.status(), StatusCode::BAD_GATEWAY);

        let body = to_bytes(resp.into_body()).await.unwrap();
        let s = std::str::from_utf8(&body).unwrap();

        assert!(s.contains("Upstream error:"));
    }

    #[tokio::test]
    async fn test_passthrough_non_json_response() {
        let (tx, rx) = oneshot::channel();

        let server = tokio::spawn(async move {
            let route =
                warp::any().map(|| Response::builder().status(200).body("plain text").unwrap());
            let (addr, fut) = warp::serve(route).bind_ephemeral(([127, 0, 0, 1], 0));
            tx.send(addr).unwrap();
            fut.await;
        });

        let addr = rx.await.unwrap();
        let uri: Uri = format!("http://{}/", addr).parse().unwrap();
        let req = Request::builder().uri(uri).body(Body::empty()).unwrap();
        let client = Client::new();
        let resp = proxy_handler(req, client).await.unwrap();

        assert_eq!(resp.status(), 200);

        let body = to_bytes(resp.into_body()).await.unwrap();

        assert_eq!(&body[..], b"plain text");

        server.abort();
    }

    #[tokio::test]
    async fn integration_proxy_injects() {
        let (tx, rx) = oneshot::channel();

        let server = tokio::spawn(async move {
            let route = warp::any().map(|| {
                let s = stream::iter(vec![
                    Ok::<Bytes, io::Error>(Bytes::from_static(b"{\"x\":42}")),
                    Ok(Bytes::from_static(b"{\"y\":99}")),
                ]);
                Response::builder()
                    .header(CONTENT_TYPE, "application/json")
                    .body(Body::wrap_stream(s))
                    .unwrap()
            });
            let (addr, fut) = warp::serve(route).bind_ephemeral(([127, 0, 0, 1], 0));
            tx.send(addr).unwrap();
            fut.await;
        });

        let addr = rx.await.unwrap();
        let uri: Uri = format!("http://{}/", addr).parse().unwrap();
        let req = Request::builder()
            .uri(uri)
            .header(CONTENT_TYPE, "application/json")
            .body(Body::empty())
            .unwrap();

        let client = Client::new();
        let resp = proxy_handler(req, client).await.unwrap();

        let out: Value =
            serde_json::from_slice(&to_bytes(resp.into_body()).await.unwrap()).unwrap();

        assert_eq!(out["x"], 42);
        assert_eq!(out["y"], 99);
        assert_eq!(out["proxied"], true);

        server.abort();
    }
}
