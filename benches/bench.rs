use jaf::merge::merge_and_inject;

use bytes::Bytes;
use criterion::{Criterion, criterion_group, criterion_main};
use futures::stream::iter;
use std::io;
use tokio::runtime::Runtime;
use tokio_util::io::StreamReader;

fn bench_merge_and_inject(c: &mut Criterion) {
    let rt: Runtime = Runtime::new().unwrap();

    c.bench_function("merge_and_inject_9000", |b: &mut criterion::Bencher<'_>| {
        b.iter(|| {
            rt.block_on(async {
                let chunks = (0..9000).map(|_| {
                    Ok::<Bytes, io::Error>(Bytes::from_static(
                        b"{\"medium_key_size\":[\"medium_value_size_9001\", 9001, 42]}",
                    ))
                });

                let reader = StreamReader::new(iter(chunks));

                merge_and_inject(reader).await.unwrap();
            })
        })
    });

    c.bench_function("merge_and_inject_900", |b: &mut criterion::Bencher<'_>| {
        b.iter(|| {
            rt.block_on(async {
                let chunks = (0..900).map(|_| {
                    Ok::<Bytes, io::Error>(Bytes::from_static(
                        b"{\"medium_key_size\":[\"medium_value_size_901\", 901, 42]}",
                    ))
                });

                let reader = StreamReader::new(iter(chunks));

                merge_and_inject(reader).await.unwrap();
            })
        })
    });

    c.bench_function("merge_and_inject_90", |b: &mut criterion::Bencher<'_>| {
        b.iter(|| {
            rt.block_on(async {
                let chunks = (0..90).map(|_| {
                    Ok::<Bytes, io::Error>(Bytes::from_static(
                        b"{\"medium_key_size\":[\"medium_value_size_91\", 91, 42]}",
                    ))
                });

                let reader = StreamReader::new(iter(chunks));

                merge_and_inject(reader).await.unwrap();
            })
        })
    });
}

criterion_group!(benches, bench_merge_and_inject);
criterion_main!(benches);
