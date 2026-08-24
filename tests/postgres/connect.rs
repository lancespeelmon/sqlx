use std::io::{self, Read};
use std::net::TcpListener;
use std::thread;
use std::time::{Duration, Instant};

use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use sqlx::{Connection, Error, PgConnection};

/// Accept a connection, read the 8-byte SSLRequest, and hang up without
/// answering it. This is what a proxy or port forwarder does when it drops a
/// connection in transit, and it needs no Postgres to reproduce.
///
/// Deliberately blocking `std::net` on its own thread rather than the async
/// runtime's listener, so the test is runtime-agnostic and runs on every leg of
/// the matrix rather than only the `tokio` one.
fn accept_then_close() -> io::Result<u16> {
    let listener = TcpListener::bind("127.0.0.1:0")?;
    let port = listener.local_addr()?.port();

    thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(mut stream) = stream else { break };

            let mut request = [0u8; 8];
            let _ = stream.read_exact(&mut request);
            drop(stream);
        }
    });

    Ok(port)
}

/// A peer that hangs up instead of answering the SSLRequest must report an EOF,
/// not a protocol violation naming a byte it never sent.
///
/// `Socket::read` resolves to `Ok(0)` at EOF without filling the buffer, so
/// before this was fixed the response buffer still held the `0u8` it was
/// initialised with, and the error read
/// `unexpected response from SSLRequest: 0x00` - which sends you looking for a
/// TLS or protocol fault that does not exist.
#[sqlx_macros::test]
async fn closing_before_answering_sslrequest_is_an_eof_not_a_protocol_error() {
    let port = accept_then_close().expect("bind the fake server");

    let options = format!("postgres://user:password@127.0.0.1:{port}/database")
        .parse::<PgConnectOptions>()
        .expect("parse the connect options");

    let error = PgConnection::connect_with(&options)
        .await
        .expect_err("connecting to a peer that hangs up must fail");

    match error {
        Error::Io(e) => assert_eq!(
            e.kind(),
            io::ErrorKind::UnexpectedEof,
            "an EOF during the SSLRequest exchange must be reported as one, got {e:?}"
        ),
        other => panic!("expected an EOF I/O error, got {other:?}"),
    }
}

/// A peer that accepts and then drops the connection must be redialled until the
/// acquire deadline, the same as one that is refusing connections outright.
///
/// Before this was fixed the pool retried exactly `ConnectionRefused` and a
/// transient `Database` error, so a connection dropped in transit - what a proxy
/// or port forwarder does when it sheds load - returned on the first attempt in
/// under a millisecond, with `acquire_timeout` barely touched.
///
/// The assertion is a FLOOR on elapsed time, not a ceiling: before the fix this
/// returns in ~1 ms, so any floor near the timeout separates the two cases
/// without being sensitive to how loaded the machine is.
#[sqlx_macros::test]
async fn a_connection_dropped_in_transit_is_retried_until_the_deadline() {
    const ACQUIRE_TIMEOUT: Duration = Duration::from_millis(750);

    let port = accept_then_close().expect("bind the fake server");
    let url = format!("postgres://user:password@127.0.0.1:{port}/database");

    let started = Instant::now();
    let error = PgPoolOptions::new()
        .max_connections(1)
        .acquire_timeout(ACQUIRE_TIMEOUT)
        .connect(&url)
        .await
        .expect_err("a peer that hangs up can never serve a connection");
    let elapsed = started.elapsed();

    assert!(
        matches!(error, Error::PoolTimedOut),
        "a retried connect should exhaust the deadline, got {error:?}"
    );
    assert!(
        elapsed >= ACQUIRE_TIMEOUT / 2,
        "expected the pool to keep retrying for about {ACQUIRE_TIMEOUT:?}, gave up after {elapsed:?}"
    );
}
