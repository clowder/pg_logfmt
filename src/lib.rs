use pgx::prelude::*;

pgx::pg_module_magic!();

#[pg_extern]
fn hello_logfmt_to_jsonb() -> &'static str {
    "Hello, logfmt_to_jsonb"
}

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use pgx::prelude::*;

    #[pg_test]
    fn test_hello_logfmt_to_jsonb() {
        assert_eq!("Hello, logfmt_to_jsonb", crate::hello_logfmt_to_jsonb());
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
