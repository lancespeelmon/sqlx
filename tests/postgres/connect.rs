use std::io::{self, Read};
use std::net::TcpListener;
use std::thread;

use sqlx::postgres::PgConnectOptions;
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
