use std::io::{Cursor, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::process::{Command, Output};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use polars::prelude::*;

// A local S3-shaped endpoint exercises the actual binary, signing, range reads,
// bucket listing, and Parquet decoding without external storage or credentials.
struct TestS3 {
    endpoint: String,
    stop: Arc<AtomicBool>,
    worker: Option<JoinHandle<()>>,
}

impl TestS3 {
    fn start() -> Self {
        let mut frame = df!("name" => ["Alice", "Bob"], "score" => [10, 20]).unwrap();
        let mut parquet = Cursor::new(Vec::new());
        ParquetWriter::new(&mut parquet).finish(&mut frame).unwrap();
        let parquet = parquet.into_inner();
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let stop = Arc::new(AtomicBool::new(false));
        let stopped = Arc::clone(&stop);
        let worker = thread::spawn(move || {
            while !stopped.load(Ordering::Relaxed) {
                match listener.accept() {
                    Ok((stream, _)) => serve(stream, &parquet),
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(5));
                    }
                    Err(error) => panic!("S3 test endpoint: {error}"),
                }
            }
        });
        Self {
            endpoint,
            stop,
            worker: Some(worker),
        }
    }

    fn run(&self, uri: &str, s3_aliases: bool) -> Output {
        let mut command = Command::new(env!("CARGO_BIN_EXE_polars-app"));
        for key in [
            "ENDPOINT",
            "REGION",
            "ACCESS_KEY_ID",
            "SECRET_ACCESS_KEY",
            "SESSION_TOKEN",
        ] {
            command.env_remove(format!("S3_{key}"));
        }
        let prefix = if s3_aliases { "S3" } else { "AWS" };
        command
            .arg(uri)
            .env(format!("{prefix}_ENDPOINT"), &self.endpoint)
            .env(format!("{prefix}_REGION"), "us-east-1")
            .env(format!("{prefix}_ACCESS_KEY_ID"), "test-key")
            .env(format!("{prefix}_SECRET_ACCESS_KEY"), "test-secret")
            .env(format!("{prefix}_SESSION_TOKEN"), "test-token")
            .env("AWS_VIRTUAL_HOSTED_STYLE_REQUEST", "false")
            .env(
                "AWS_ENDPOINT_URL_S3",
                if s3_aliases {
                    "http://127.0.0.1:1"
                } else {
                    &self.endpoint
                },
            )
            .output()
            .unwrap()
    }
}

impl Drop for TestS3 {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        let result = self.worker.take().unwrap().join();
        // Preserve the original test failure instead of aborting on a second
        // panic when the server thread also failed.
        if !thread::panicking() {
            result.unwrap();
        }
    }
}

fn serve(mut stream: TcpStream, parquet: &[u8]) {
    // Accepted sockets inherit the listener's nonblocking mode on macOS.
    // This handler uses blocking reads and writes with a read timeout.
    stream.set_nonblocking(false).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(10)))
        .unwrap();
    let mut request = Vec::new();
    let mut buf = [0; 4096];
    while !request.windows(4).any(|part| part == b"\r\n\r\n") {
        let count = stream.read(&mut buf).unwrap();
        if count == 0 {
            return;
        }
        request.extend_from_slice(&buf[..count]);
    }
    let request = String::from_utf8(request).unwrap();
    let mut parts = request.lines().next().unwrap().split_whitespace();
    let method = parts.next().unwrap();
    let target = parts.next().unwrap();
    assert!(
        request
            .to_ascii_lowercase()
            .contains("authorization: aws4-hmac-sha256")
    );
    assert!(
        request
            .to_ascii_lowercase()
            .contains("x-amz-security-token: test-token")
    );

    let listing = format!(
        "<ListBucketResult><Name>logs</Name><IsTruncated>false</IsTruncated>{}</ListBucketResult>",
        ["parsed/part-00000.parquet", "parsed/part-00001.parquet"]
            .map(|key| format!(
                "<Contents><Key>{key}</Key><LastModified>2026-01-01T00:00:00Z</LastModified>\
                <ETag>\"test\"</ETag><Size>{}</Size></Contents>",
                parquet.len()
            ))
            .join("")
    );
    let path = target.split('?').next().unwrap();
    let (status, body) = if target.contains("list-type=2") {
        ("200 OK", listing.as_bytes())
    } else if [
        "/logs/parsed/part-00000.parquet",
        "/logs/parsed/part-00001.parquet",
    ]
    .contains(&path)
    {
        ("200 OK", parquet)
    } else if path == "/logs/invalid.parquet" {
        ("200 OK", b"this is not parquet".as_slice())
    } else {
        (
            "404 Not Found",
            b"<Error><Code>NoSuchKey</Code></Error>".as_slice(),
        )
    };
    let range = request.lines().find_map(|line| {
        line.to_ascii_lowercase()
            .strip_prefix("range: bytes=")
            .map(str::to_owned)
    });
    let (status, body, content_range) = if status == "200 OK"
        && method == "GET"
        && let Some(range) = range
    {
        let (start, end) = range.split_once('-').unwrap();
        let start: usize = start.parse().unwrap();
        let end: usize = if end.is_empty() {
            body.len() - 1
        } else {
            end.parse().unwrap()
        };
        (
            "206 Partial Content",
            &body[start..=end],
            format!("Content-Range: bytes {start}-{end}/{}\r\n", body.len()),
        )
    } else {
        (status, body, String::new())
    };
    write!(
        stream,
        "HTTP/1.1 {status}\r\nContent-Length: {}\r\n\
        Last-Modified: Thu, 01 Jan 2026 00:00:00 GMT\r\nETag: \"test\"\r\n\
        {content_range}Connection: close\r\n\r\n",
        body.len()
    )
    .unwrap();
    if method != "HEAD" {
        stream.write_all(body).unwrap();
    }
}

#[test]
fn serves_delayed_requests_on_nonblocking_connections() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let mut client = TcpStream::connect(listener.local_addr().unwrap()).unwrap();
    client
        .set_read_timeout(Some(Duration::from_secs(10)))
        .unwrap();
    let (ready, accepted) = std::sync::mpsc::channel();
    let worker = thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        // Reproduce macOS inheriting nonblocking mode from the listener,
        // even when this regression test runs on Linux.
        stream.set_nonblocking(true).unwrap();
        ready.send(()).unwrap();
        serve(stream, b"fixture");
    });
    accepted.recv_timeout(Duration::from_secs(10)).unwrap();
    thread::sleep(Duration::from_millis(50));
    let sent = client.write_all(
        b"HEAD /logs/parsed/part-00000.parquet HTTP/1.1\r\n\
        Host: localhost\r\nAuthorization: AWS4-HMAC-SHA256 test\r\n\
        x-amz-security-token: test-token\r\n\r\n",
    );
    let mut response = String::new();
    let received = client.read_to_string(&mut response);
    worker.join().unwrap();
    sent.unwrap();
    received.unwrap();
    assert!(response.starts_with("HTTP/1.1 200 OK\r\n"), "{response}");
    assert!(response.contains("Content-Length: 7\r\n"), "{response}");
    assert!(
        response.ends_with("\r\n\r\n"),
        "HEAD must not return a body"
    );
}

#[test]
fn reads_s3_parquet_and_reports_storage_and_format_errors() {
    let server = TestS3::start();
    for (uri, shape, s3_aliases) in [
        ("s3://logs/parsed/part-00000.parquet", "(2, 2)", false),
        ("s3://logs/parsed/part-00000.parquet", "(2, 2)", true),
        ("s3://logs/parsed/*.parquet", "(4, 2)", true),
    ] {
        let output = server.run(uri, s3_aliases);
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        let stdout = String::from_utf8(output.stdout).unwrap();
        assert!(stdout.contains(shape), "{stdout}");
        assert!(stdout.contains("Alice"), "{stdout}");
        assert!(stdout.contains("Bob"), "{stdout}");
    }
    for uri in ["s3://logs/missing.parquet", "s3://logs/invalid.parquet"] {
        let output = server.run(uri, true);
        assert!(!output.status.success());
        assert!(output.stdout.is_empty());
        assert!(String::from_utf8_lossy(&output.stderr).contains("error:"));
    }
}
