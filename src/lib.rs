use pgx::prelude::*;
use pgx::{JsonB};
use serde_json::json;
use crate::parser::parse;

pgx::pg_module_magic!();

pub mod parser;

#[pg_extern]
fn logfmt_to_jsonb(value: &str) -> JsonB {
    let parsed = parse(value);
    JsonB(json!(parsed))
}

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use pgx::prelude::*;
    use serde_json::json;

    #[pg_test]
    fn test_logfmt_to_jsonb() {
        let logline = "source=web.1 dyno=heroku.238235071.aa92a0d0-09a3-4b15-a717-a2821dd241f7 sample#load_avg_1m=0.57 sample#load_avg_5m=0.16 sample#load_avg_15m=0.07";

        assert_eq!(
            json!({
                "source": "web.1",
                "dyno": "heroku.238235071.aa92a0d0-09a3-4b15-a717-a2821dd241f7",
                "sample#load_avg_1m": "0.57",
                "sample#load_avg_5m": "0.16",
                "sample#load_avg_15m": "0.07"
            }),
            json!(crate::logfmt_to_jsonb(logline))
        );
    }

}

/// This module is required by `cargo pgx test` invocations.
/// It must be visible at the root of your extension crate.
#[cfg(test)]
pub mod pg_test {
    pub fn setup(_options: Vec<&str>) {
        // perform one-off initialization when the pg_test framework starts
    }

    pub fn postgresql_conf_options() -> Vec<&'static str> {
        // return any postgresql.conf settings that are required for your tests
        vec![]
    }
}
