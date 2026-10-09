use std::env;
use std::process::ExitCode;

use polars::io::cloud::{AmazonS3ConfigKey, CloudOptions};
use polars::prelude::*;

const USAGE: &str = "Usage: polars-app [s3://bucket/key.parquet]\n\
    Reads and prints Parquet data from S3. Quote glob patterns to read multiple files.\n\
    Without an argument, prints the sample DataFrame.\n\
    Configure credentials with AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, and\n\
    optionally AWS_SESSION_TOKEN. Set AWS_REGION and AWS_ENDPOINT for custom storage.\n\
    S3_ENDPOINT, S3_REGION, S3_ACCESS_KEY_ID, S3_SECRET_ACCESS_KEY, and S3_SESSION_TOKEN\n\
    override the corresponding AWS configuration. See README.md for examples.";

#[derive(Debug, PartialEq)]
enum Command {
    Example,
    Help,
    Read(String),
}

fn parse_args(args: impl IntoIterator<Item = String>) -> Result<Command, String> {
    let mut args = args.into_iter();
    let Some(arg) = args.next() else {
        return Ok(Command::Example);
    };
    if args.next().is_some() {
        return Err("expected a single S3 URI".into());
    }
    if arg == "--help" || arg == "-h" {
        return Ok(Command::Help);
    }
    let Some(path) = arg.strip_prefix("s3://") else {
        return Err("input must be an s3://bucket/key.parquet URI".into());
    };
    match path.split_once('/') {
        Some((bucket, key)) if !bucket.is_empty() && !key.is_empty() => Ok(Command::Read(arg)),
        _ => Err("S3 URI must include a bucket and an object key or glob pattern".into()),
    }
}

fn cloud_options(get_env: impl Fn(&str) -> Option<String>) -> CloudOptions {
    let aliases = [
        // Set both keys: object_store prioritizes the S3-specific endpoint.
        ("S3_ENDPOINT", AmazonS3ConfigKey::Endpoint),
        ("S3_ENDPOINT", AmazonS3ConfigKey::S3Endpoint),
        ("S3_REGION", AmazonS3ConfigKey::Region),
        ("S3_ACCESS_KEY_ID", AmazonS3ConfigKey::AccessKeyId),
        ("S3_SECRET_ACCESS_KEY", AmazonS3ConfigKey::SecretAccessKey),
        ("S3_SESSION_TOKEN", AmazonS3ConfigKey::Token),
    ];
    CloudOptions::default().with_aws(aliases.into_iter().filter_map(|(name, key)| {
        get_env(name)
            .filter(|value| !value.is_empty())
            .map(|value| (key, value))
    }))
}

fn read_parquet(uri: &str, options: CloudOptions) -> PolarsResult<DataFrame> {
    let args = ScanArgsParquet {
        cloud_options: Some(options),
        ..Default::default()
    };
    LazyFrame::scan_parquet(PlRefPath::new(uri), args)?.collect()
}

fn example() -> PolarsResult<DataFrame> {
    df!(
        "name" => ["Alice", "Bob", "Charlie"],
        "score" => [10, 20, 30],
    )
}

fn main() -> ExitCode {
    let command = match parse_args(env::args().skip(1)) {
        Ok(command) => command,
        Err(error) => {
            eprintln!("error: {error}\n\n{USAGE}");
            return ExitCode::FAILURE;
        }
    };
    let result = match command {
        Command::Help => {
            println!("{USAGE}");
            return ExitCode::SUCCESS;
        }
        Command::Example => example(),
        Command::Read(uri) => read_parquet(&uri, cloud_options(|name| env::var(name).ok())),
    };
    match result {
        Ok(frame) => {
            println!("{frame}");
            ExitCode::SUCCESS
        }
        Err(error) => {
            eprintln!("error: {error}");
            ExitCode::FAILURE
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use polars::io::cloud::CloudConfig;

    fn parse(args: &[&str]) -> Result<Command, String> {
        parse_args(args.iter().map(|arg| (*arg).to_owned()))
    }

    #[test]
    fn accepts_example_help_and_s3_inputs() {
        assert_eq!(parse(&[]), Ok(Command::Example));
        assert_eq!(parse(&["--help"]), Ok(Command::Help));
        assert_eq!(parse(&["-h"]), Ok(Command::Help));
        for uri in ["s3://logs/part.parquet", "s3://logs/parsed/**/*.parquet"] {
            assert_eq!(parse(&[uri]), Ok(Command::Read(uri.into())));
        }
    }

    #[test]
    fn rejects_invalid_inputs_before_accessing_storage() {
        for args in [
            vec!["s3://"],
            vec!["s3://logs"],
            vec!["s3://logs/"],
            vec!["s3:///part.parquet"],
            vec!["https://logs/part.parquet"],
            vec!["part.parquet"],
            vec!["--unknown"],
            vec!["s3://logs/part.parquet", "extra"],
        ] {
            assert!(parse(&args).is_err(), "{args:?}");
        }
    }

    #[test]
    fn maps_s3_environment_aliases_and_ignores_empty_values() {
        let options = cloud_options(|name| match name {
            "S3_ENDPOINT" => Some("https://storage.example.com".into()),
            "S3_REGION" => Some("us-east-1".into()),
            "S3_ACCESS_KEY_ID" => Some("test-key".into()),
            "S3_SECRET_ACCESS_KEY" => Some("test-secret".into()),
            "S3_SESSION_TOKEN" => Some(String::new()),
            _ => None,
        });
        let Some(CloudConfig::Aws(config)) = options.config else {
            panic!("expected AWS options");
        };
        assert_eq!(config.len(), 5);
        assert!(config.contains(&(
            AmazonS3ConfigKey::Endpoint,
            "https://storage.example.com".into()
        )));
        assert!(config.contains(&(
            AmazonS3ConfigKey::S3Endpoint,
            "https://storage.example.com".into()
        )));
        assert!(config.contains(&(AmazonS3ConfigKey::Region, "us-east-1".into())));
        assert!(config.contains(&(AmazonS3ConfigKey::AccessKeyId, "test-key".into())));
        assert!(config.contains(&(AmazonS3ConfigKey::SecretAccessKey, "test-secret".into())));
    }
}
