use crate::parser::parse;
use pgrx::prelude::*;
use pgrx::JsonB;
use serde_json::json;

pgrx::pg_module_magic!();

pub mod parser;

#[pg_extern]
fn logfmt_to_jsonb(value: &str) -> Option<JsonB> {
    let parsed = parse(value);

    parsed.map(|v| JsonB(json!(v)))
}

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use pgrx::prelude::*;
    use pgrx::JsonB;
    use std::collections::HashMap;

    fn pair(key: &str, val: Option<&str>) -> (String, Option<String>) {
        match val {
            Some(v) => (key.to_string(), Some(v.to_string())),
            None => (key.to_string(), None),
        }
    }

    #[pg_test]
    fn test_logfmt_to_jsonb() {
        let result: Option<JsonB> = Spi::get_one::<JsonB>("SELECT logfmt_to_jsonb('source=web.1 dyno=heroku.238235071.aa92a0d0-09a3-4b15-a717-a2821dd241f7 sample#load_avg_1m=0.57 sample#load_avg_5m=0.16 sample#load_avg_15m=0.07');").expect("error fetching from database");
        let json: JsonB = result.expect("database returned `NULL`");
        let parsed: HashMap<String, Option<String>> =
            serde_json::from_value(json.0).expect("error interpreting data");

        assert_eq!(
            HashMap::from([
                pair("source", Some("web.1")),
                pair(
                    "dyno",
                    Some("heroku.238235071.aa92a0d0-09a3-4b15-a717-a2821dd241f7")
                ),
                pair("sample#load_avg_1m", Some("0.57")),
                pair("sample#load_avg_5m", Some("0.16")),
                pair("sample#load_avg_15m", Some("0.07"))
            ]),
            parsed
        );
    }

    #[pg_test]
    fn test_parsing_normal_logs_returns_null() {
        let result: Option<JsonB> = Spi::get_one::<JsonB>("SELECT logfmt_to_jsonb('I, [2023-04-21T13:13:00.953378 #2] INFO -- : [FOOBAR] Reporting 2 metrics');")
            .expect("error fetching from database");

        assert!(result.is_none())
    }
}

/// This module is required by `cargo pgrx test` invocations.
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
