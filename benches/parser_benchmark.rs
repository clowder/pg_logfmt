use criterion::{black_box, criterion_group, criterion_main, Criterion};
use pg_logfmt::parser::parse;

fn parser_benchmark(c: &mut Criterion) {
    c.bench_function("parse", |b| b.iter(|| {
        parse(black_box("at=info method=POST path=\"/foo/bar\" host=example.com request_id=f116113c-b8ed-41ea-bbf3-a031313dd936 fwd=\"0.0.0.0\" dyno=web.1 connect=0ms service=25ms status=204 bytes=490 protocol=http"))
    }));
}

criterion_group!(benches, parser_benchmark);
criterion_main!(benches);
