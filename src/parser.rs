use std::collections::HashMap;

fn complete_pair(buf: String, pair: Option<(String,String)>) -> (String, Option<String>) {
    match pair {
        Some((key, _value)) =>
            (key, Some(buf)),
        None =>
            (buf, None),
    }
}

pub fn parse(message: &str) -> HashMap<String, Option<String>> {
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

#[cfg(test)]
mod tests {
    use crate::parser::*;

    fn pair(key: &str, val: Option<&str>) -> (String, Option<String>) {
        match val {
            Some(v) =>
                (key.to_string(), Some(v.to_string())),
            None =>
                (key.to_string(), None),
        }
    }

    #[test]
    fn it_parses() {
        assert_eq!(HashMap::from([
                pair("a", Some("1")),
                pair("b", Some("bar")),
                pair("ƒ", Some("2h3s")),
                pair("r", Some("esc\t")),
                pair("d", None),
                pair("x", Some("sf"))
        ]), parse("a=1 b=\"bar\" ƒ=2h3s r=\"esc\t\" d x=sf"));

        assert_eq!(HashMap::from([
            pair("x", Some(""))
        ]), parse("x= "));

        assert_eq!(HashMap::from([
            pair("y", Some(""))
        ]), parse("y="));

        assert_eq!(HashMap::from([
            pair("y", None)
        ]), parse("y"));

        assert_eq!(HashMap::from([
            pair("y", None)
        ]), parse("y"));

        assert_eq!(HashMap::from([
            pair("y", Some("f"))
        ]), parse("y=f"));

        assert_eq!(HashMap::from([
            pair("y", Some("f"))
        ]), parse("y=\"f\""));

        assert_eq!(HashMap::from([
            pair("y", Some("f(\"x\")"))
        ]), parse("y=\"f(\\\"x\\\")"));

        // unknown escapes just get written to value
        assert_eq!(HashMap::from([
            pair("y", Some("\\x"))
        ]), parse("y=\\x"));

        // this is considered garbage and produces nothing
        assert_eq!(HashMap::new(), parse("=y"));
    }
}
