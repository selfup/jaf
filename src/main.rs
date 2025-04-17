// Cargo.toml dependencies:
// [dependencies]
// tokio = { version = "1", features = ["full"] }
// hyper = { version = "0.14", features = ["full"] }
// serde_json = "1.0"
// tokio-util = { version = "0.7", features = ["io"] }
// futures = "0.3"
// bytes = "1.0"
// [dev-dependencies]
// warp = "0.3"

use bytes::Bytes;
use futures::TryStreamExt;
use hyper::{
    Client, Request, Response, Server, Uri,
    body::Body,
    client::HttpConnector,
    header::CONTENT_TYPE,
    service::{make_service_fn, service_fn},
};
use serde_json::de::Deserializer;
use serde_json::{Value, json};
use std::convert::Infallible;
use std::marker::Unpin;
use tokio::io::{AsyncRead, AsyncReadExt};
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
    // Only rewrite requests sent to the proxy itself; others go unchanged
    if let Some(authority) = req.uri().authority().map(|a| a.as_str()) {
        if authority == "127.0.0.1:3000" {
            let mut parts = req.uri().clone().into_parts();
            parts.scheme = Some("https".parse().unwrap());
            parts.authority = Some("api.example.com".parse().unwrap());
            *req.uri_mut() = Uri::from_parts(parts).unwrap();
        }
    }

    // Stream and mutate JSON request body
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

    // Forward to upstream
    let resp = match client.request(req).await {
        Ok(r) => r,
        Err(e) => {
            return Ok(Response::builder()
                .status(502)
                .body(Body::from(format!("Upstream error: {}", e)))
                .unwrap());
        }
    };

    // Stream and mutate JSON response body
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

    // Fallback: passthrough
    Ok(resp)
}

/// Reads an AsyncRead stream of concatenated JSON objects,
/// merges them into one object, and injects "streamed": true.
async fn merge_and_inject<R>(
    mut reader: R,
) -> Result<Value, Box<dyn std::error::Error + Send + Sync>>
where
    R: AsyncRead + Unpin,
{
    let mut buf = Vec::new();
    let mut temp = [0u8; 8192];
    let mut items: Vec<serde_json::Map<String, Value>> = Vec::new();

    loop {
        let n = reader.read(&mut temp).await?;
        if n == 0 {
            break;
        }
        buf.extend_from_slice(&temp[..n]);

        let mut de = Deserializer::from_slice(&buf).into_iter::<Value>();
        let mut offset = 0;
        while let Some(Ok(val)) = de.next() {
            if let Value::Object(map) = val {
                items.push(map);
            }
            offset = de.byte_offset();
        }
        buf = buf.split_off(offset);
    }

    let mut combined = serde_json::Map::new();
    for map in items {
        for (k, v) in map {
            combined.insert(k, v);
        }
    }
    combined.insert("streamed".to_string(), json!(true));
    Ok(Value::Object(combined))
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use futures::stream;
    use hyper::{Body, Client, Request, body::to_bytes, header::CONTENT_TYPE};
    use tokio::sync::oneshot;
    use warp::Filter;

    #[tokio::test]
    async fn integration_proxy_injects() {
        // Start dummy upstream
        let (tx, rx) = oneshot::channel();
        let server = tokio::spawn(async move {
            let route = warp::any().map(|| {
                let s = stream::iter(vec![
                    Ok::<Bytes, std::convert::Infallible>(Bytes::from_static(b"{\"x\":42}")),
                    Ok::<Bytes, std::convert::Infallible>(Bytes::from_static(b"{\"y\":99}")),
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
        // Direct proxy to test server
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
