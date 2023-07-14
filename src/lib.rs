use crate::parser::parse;
use pgrx::prelude::*;
use pgrx::JsonB;
use serde_json::{Map, Value};
use std::iter;

pgrx::pg_module_magic!();

pub mod parser;

#[pg_extern(immutable, parallel_safe)]
fn logfmt_to_jsonb(value: &str) -> Option<JsonB> {
    let parsed = parse(value);

    parsed.map(|v| {
        let map = v.into_iter().fold(Map::new(), |mut acc, (key, value)| {
            acc.insert(
                key.to_string(),
                Value::from(value.map(|v| v.replace("\\\"", "\""))),
            );
            acc
        });
        JsonB(serde_json::Value::Object(map))
    })
}

#[pg_extern(immutable, parallel_safe)]
fn logfmt_keys<'a>(value: &'a str) -> SetOfIterator<'a, &'a str> {
    match parse(value) {
        Some(v) => SetOfIterator::new(v.into_iter().map(|(key, _value)| key)),
        None => SetOfIterator::new(iter::empty::<&str>()),
    }
}

#[pg_extern(immutable, parallel_safe)]
fn logfmt_keys_array<'a>(value: &'a str) -> Option<Vec<&'a str>> {
    match parse(value) {
        Some(v) => Some(v.into_iter().map(|(key, _value)| key).collect()),
        None => None,
    }
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
    fn test_logfmt_to_jsonb_returns_null_for_normal_log_lines() {
        let result: Option<JsonB> = Spi::get_one::<JsonB>("SELECT logfmt_to_jsonb('I, [2023-04-21T13:13:00.953378 #2] INFO -- : [FOOBAR] Reporting 2 metrics');")
            .expect("error fetching from database");

        assert!(result.is_none())
    }

    #[pg_test]
    fn test_logfmt_to_jsonb_unescapes_double_escaped_quotes() {
        let result: Option<JsonB> =
            Spi::get_one::<JsonB>("SELECT logfmt_to_jsonb('y=\"f(\\\"x\\\")\"')")
                .expect("error fetching from database");
        let json: JsonB = result.expect("database returned `NULL`");
        let parsed: HashMap<String, Option<String>> =
            serde_json::from_value(json.0).expect("error interpreting data");

        assert_eq!(HashMap::from([pair("y", Some("f(\"x\")")),]), parsed);
    }

    #[pg_test]
    fn test_logfmt_keys() {
        Spi::connect(|client| {
            let result = client.select("SELECT logfmt_keys('source=web.1 dyno=heroku.238235071.aa92a0d0-09a3-4b15-a717-a2821dd241f7 sample#load_avg_1m=0.57 sample#load_avg_5m=0.16 sample#load_avg_15m=0.07')", None, None).expect("error fetching from database");

            assert_eq!(
                vec![
                    "source",
                    "dyno",
                    "sample#load_avg_1m",
                    "sample#load_avg_5m",
                    "sample#load_avg_15m",
                ],
                result
                    .into_iter()
                    .map(|x| x
                        .get_by_name::<String, &str>("logfmt_keys")
                        .expect("error fetching datum")
                        .expect("datum was NULL"))
                    .collect::<Vec<String>>()
            );
        });
    }

    #[pg_test]
    fn test_logfmt_keys_returns_nothing_for_normal_log_lines() {
        Spi::connect(|client| {
            let result = client.select("SELECT logfmt_keys('I, [2023-04-21T13:13:00.953378 #2] INFO -- : [FOOBAR] Reporting 2 metrics');", None, None).expect("error fetching from database");

            assert_eq!(0, result.len());
        });
    }

    #[pg_test]
    fn test_logfmt_keys_array() {
        let result = Spi::get_one::<Vec<String>>("SELECT logfmt_keys_array('source=web.1 dyno=heroku.238235071.aa92a0d0-09a3-4b15-a717-a2821dd241f7 sample#load_avg_1m=0.57 sample#load_avg_5m=0.16 sample#load_avg_15m=0.07');").expect("error fetching from database");
        let keys = result.expect("database returned `NULL`");

        assert_eq!(
            vec![
                "source",
                "dyno",
                "sample#load_avg_1m",
                "sample#load_avg_5m",
                "sample#load_avg_15m",
            ],
            keys
        );
    }

    #[pg_test]
    fn test_logfmt_keys_array_returns_null_for_normal_log_lines() {
        let result = Spi::get_one::<Vec<String>>("SELECT logfmt_keys_array('I, [2023-04-21T13:13:00.953378 #2] INFO -- : [FOOBAR] Reporting 2 metrics');")
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
