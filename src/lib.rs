use pgx::prelude::*;
use pgx::{JsonB};
use serde_json::json;
use std::collections::HashMap;

pgx::pg_module_magic!();

fn complete_pair(buf: String, pair: Option<(String,String)>) -> (String,String) {
    match pair {
        Some((key, _value)) =>
            (key, buf),
        None =>
            (buf, String::new()),
    }
}

pub fn parse(message: &str) -> HashMap<String, String> {
    let mut pair: Option<(String,String)> = None;
    let mut pairs = HashMap::new();
    let mut buf = String::new();

    let mut escape = false;
    let mut garbage = false;
    let mut quoted = false;

    for c in message.chars() {
        match (quoted, c) {
            (false, ' ') => {
                if !buf.is_empty() {
                    if !garbage {
                        // the buffer that we just processed is either a value
                        // or a valueless key depending on the current state of
                        // `pair`
                        let (k,v) = complete_pair(buf, pair);
                        pairs.insert(k,v);

                        pair = None;
                    }
                    buf = String::new();
                }
                garbage = false;
            },
            (false, '=') => {
                if !buf.is_empty() {
                    pair = Some((buf, String::from("")));
                    buf = String::new();
                } else {
                    garbage = true;
                }
            },
            (true, '\\') => {
                escape = true;
            }
            (_, '"') => {
                if escape {
                    buf.push(c);
                    escape = false;
                } else {
                    quoted = !quoted;
                }
            },
            _ => {
                // if the last character we read was an escape, but this
                // character was not a quote, then store the escape back into the
                // buffer
                if escape {
                    buf.push('\\');
                    escape = false;
                }
                buf.push(c);
            },
        }
    }

    // and process one final time at the end of the message to get the last
    // data point
    if !garbage {
        let (k,v) = complete_pair(buf, pair);
        pairs.insert(k,v);
    }

    pairs
}

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
