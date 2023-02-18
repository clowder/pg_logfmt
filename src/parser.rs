use std::collections::HashMap;

use nom::{
    branch::alt,
    bytes::complete::{escaped_transform, tag, take_while1},
    character::complete::{none_of, one_of, space0},
    combinator::{eof, opt},
    multi::fold_many1,
    sequence::{delimited, preceded, tuple},
    IResult,
};

fn quoted_value(input: &str) -> IResult<&str, String> {
    let (rest, value) = delimited(
        tag("\""),
        opt(escaped_transform(none_of("\"\\"), '\\', one_of("\"\\"))),
        alt((tag("\""), eof)),
    )(input)?;

    match value {
        Some(value) => Ok((rest, value)),
        None => Ok((rest, String::new())),
    }
}

fn bare_value(input: &str) -> IResult<&str, String> {
    let (rest, value) = take_while1(|c| c != ' ')(input)?;

    Ok((rest, value.to_string()))
}

fn pair(input: &str) -> IResult<&str, (String, Option<String>)> {
    let key = take_while1(|c| c != '=' && c != ' ');
    let value = preceded(tag("="), alt((quoted_value, bare_value)));
    let (rest, (k, v)) = delimited(space0, tuple((key, opt(value))), space0)(input)?;

    Ok((rest, (k.to_string(), v)))
}

fn pairs(input: &str) -> IResult<&str, HashMap<String, Option<String>>> {
    fold_many1(
        pair,
        HashMap::new,
        |mut acc: HashMap<String, Option<String>>, (key, value)| {
            acc.insert(key, value);
            acc
        },
    )(input)
}

pub fn parse(message: &str) -> Option<HashMap<String, Option<String>>> {
    pairs(message).map(|(_, result)| result).ok()
}

#[cfg(test)]
mod tests {
    use crate::parser::*;

    fn pair(key: &str, val: Option<&str>) -> (String, Option<String>) {
        match val {
            Some(v) => (key.to_string(), Some(v.to_string())),
            None => (key.to_string(), None),
        }
    }

    #[test]
    fn test_heroku_metrics_lines() {
        assert_eq!(
            Some(HashMap::from([
                pair("source", Some("web.1")),
                pair("dyno", Some("heroku.238235071.aa92a0d0-09a3-4b15-a717-a2821dd241f7")),
                pair("sample#load_avg_1m", Some("0.57")),
                pair("sample#load_avg_5m", Some("0.16")),
                pair("sample#load_avg_15m", Some("0.07"))
            ])),
            parse("source=web.1 dyno=heroku.238235071.aa92a0d0-09a3-4b15-a717-a2821dd241f7 sample#load_avg_1m=0.57 sample#load_avg_5m=0.16 sample#load_avg_15m=0.07")
        );
    }

    #[test]
    fn test_lograge_lines() {
        assert_eq!(
            Some(HashMap::from([
                pair("at", Some("info")),
                pair("method", Some("POST")),
                pair("path", Some("/foo/bar")),
                pair("host", Some("example.com")),
                pair("request_id", Some("f116113c-b8ed-41ea-bbf3-a031313dd936")),
                pair("fwd", Some("0.0.0.0")),
                pair("dyno", Some("web.1")),
                pair("connect", Some("0ms")),
                pair("service", Some("25ms")),
                pair("status", Some("204")),
                pair("bytes", Some("490")),
                pair("protocol", Some("http")),
            ])),
            parse("at=info method=POST path=\"/foo/bar\" host=example.com request_id=f116113c-b8ed-41ea-bbf3-a031313dd936 fwd=\"0.0.0.0\" dyno=web.1 connect=0ms service=25ms status=204 bytes=490 protocol=http")
        );
    }

    #[test]
    fn test_edge_cases() {
        // leading whitespace is discarded
        assert_eq!(
            Some(HashMap::from([pair("foo", Some("bar"))])),
            parse("  foo=bar")
        );

        // unicode works as expected
        assert_eq!(
            Some(HashMap::from([pair("ƒ", Some("2h3s"))])),
            parse("ƒ=2h3s")
        );

        // blank values are `None` unless they're quoted strings
        assert_eq!(Some(HashMap::from([pair("x", None)])), parse("x= "));

        assert_eq!(Some(HashMap::from([pair("y", None)])), parse("y="));

        assert_eq!(Some(HashMap::from([pair("y", Some(""))])), parse("y=\"\""));

        // double escaped quotes are left in tact
        assert_eq!(
            Some(HashMap::from([pair("y", Some("f(\"x\")"))])),
            parse("y=\"f(\\\"x\\\")\"")
        );

        // missing closing quote consumes to eof
        assert_eq!(
            Some(HashMap::from([pair("y", Some(" a=b"))])),
            parse("y=\" a=b")
        );

        // unknown escapes just get written to value
        assert_eq!(
            Some(HashMap::from([pair("y", Some("\\x"))])),
            parse("y=\\x")
        );

        // this is considered garbage and produces nothing
        assert_eq!(None, parse("=y"));
    }
}
