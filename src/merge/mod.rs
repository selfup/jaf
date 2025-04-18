use serde_json::de::Deserializer;
use serde_json::{Value, json};
use std::marker::Unpin;
use tokio::io::{AsyncRead, AsyncReadExt};

/// Reads an AsyncRead stream of concatenated JSON objects,
/// merges into one object, and injects "streamed": true.
pub async fn merge_and_inject<R>(
    mut reader: R,
) -> Result<Value, Box<dyn std::error::Error + Send + Sync>>
where
    R: AsyncRead + Unpin,
{
    let mut buf: Vec<u8> = Vec::new();
    let mut temp: [u8; 8192] = [0u8; 8192];
    let mut items: Vec<serde_json::Map<String, Value>> = Vec::new();

    loop {
        let n: usize = reader.read(&mut temp).await?;

        if n == 0 {
            break;
        }

        buf.extend_from_slice(&temp[..n]);

        let mut de: serde_json::StreamDeserializer<'_, serde_json::de::SliceRead<'_>, Value> =
            Deserializer::from_slice(&buf).into_iter::<Value>();

        let mut offset: usize = 0;

        while let Some(res) = de.next() {
            match res {
                Ok(Value::Object(map)) => items.push(map),
                Ok(_) => {}
                Err(err) => {
                    // If it's an EOF-incomplete error, wait for more data. Otherwise, propagate.
                    if err.classify() == serde_json::error::Category::Eof {
                        break;
                    } else {
                        return Err(Box::new(err));
                    }
                }
            }

            offset = de.byte_offset();
        }

        buf = buf.split_off(offset);
    }

    let mut combined: serde_json::Map<String, Value> = serde_json::Map::new();
    for map in items {
        combined.extend(map);
    }

    combined.insert("streamed".to_string(), json!(true));

    Ok(Value::Object(combined))
}

#[cfg(test)]
mod merge_tests {
    use super::*;
    use bytes::Bytes;
    use futures::stream;
    use std::io;
    use tokio_util::io::StreamReader;

    #[tokio::test]
    async fn test_merge_and_inject_fragmented() {
        let chunks: Vec<Result<Bytes, io::Error>> = vec![
            Ok(Bytes::from_static(b"{\"a\":1")),
            Ok(Bytes::from_static(b"}{\"b\":2}")),
        ];

        let reader: StreamReader<
            stream::Iter<std::vec::IntoIter<Result<Bytes, io::Error>>>,
            Bytes,
        > = StreamReader::new(stream::iter(chunks));

        let merged: Value = merge_and_inject(reader).await.unwrap();

        assert_eq!(merged["a"], 1);
        assert_eq!(merged["b"], 2);
        assert_eq!(merged["streamed"], true);
    }

    #[tokio::test]
    async fn test_merge_and_inject_empty() {
        let reader: StreamReader<
            stream::Iter<std::vec::IntoIter<Result<Bytes, io::Error>>>,
            Bytes,
        > = StreamReader::new(stream::iter(Vec::<Result<Bytes, io::Error>>::new()));

        let merged: Value = merge_and_inject(reader).await.unwrap();

        let obj: &serde_json::Map<String, Value> = merged.as_object().unwrap();

        assert_eq!(obj.len(), 1);
        assert_eq!(obj["streamed"], true);
    }

    #[tokio::test]
    async fn test_merge_and_inject_invalid() {
        let chunks: Vec<Result<Bytes, io::Error>> = vec![Ok(Bytes::from_static(b"not json"))];

        let reader: StreamReader<
            stream::Iter<std::vec::IntoIter<Result<Bytes, io::Error>>>,
            Bytes,
        > = StreamReader::new(stream::iter(chunks));

        let res = merge_and_inject(reader).await;

        assert!(res.is_err());
    }
}
