//! A fake of the EigenDA proxy's API for tests: `POST /put` stores the
//! payload under a certificate, `GET /get/<hex cert>` returns it.
//!
//! The certificate is `0x03` (the current version byte) followed by the
//! payload's keccak; a real one is an RLP body of about 400 bytes that the
//! verifier contract understands. The real proxy answers an unknown
//! certificate with 500; this fake answers 404. The client treats every
//! non-success alike. The tests exercise the batcher's side of the API, not
//! EigenDA: the same client code reaches the real proxy on a deployment.
//! One thread serves one connection at a time; a test's traffic is small.

use std::collections::HashMap;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::{Arc, Mutex};
use std::thread;

use alloy_primitives::{Bytes, hex, keccak256};

/// The fake proxy. It serves until dropped.
pub struct FakeDaProxy {
    addr: SocketAddr,
    store: Arc<Mutex<HashMap<Bytes, Vec<u8>>>>,
}

struct Request {
    method: String,
    path: String,
    body: Vec<u8>,
}

impl Request {
    fn read(stream: &mut TcpStream) -> Option<Self> {
        let mut reader = BufReader::new(stream.try_clone().ok()?);
        let mut line = String::new();
        reader.read_line(&mut line).ok()?;
        let mut parts = line.split_whitespace();
        let method = parts.next()?.to_string();
        let path = parts.next()?.to_string();
        let mut length = 0usize;
        loop {
            let mut header = String::new();
            reader.read_line(&mut header).ok()?;
            let header = header.trim_end();
            if header.is_empty() {
                break;
            }
            if let Some(v) = header
                .split_once(':')
                .filter(|(k, _)| k.eq_ignore_ascii_case("content-length"))
                .map(|(_, v)| v.trim())
            {
                length = v.parse().ok()?;
            }
        }
        let mut body = vec![0u8; length];
        reader.read_exact(&mut body).ok()?;
        Some(Self { method, path, body })
    }
}

impl FakeDaProxy {
    /// Start on a free loopback port.
    ///
    /// # Panics
    /// Panics when no port can be bound.
    #[must_use]
    pub fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind a loopback port");
        let addr = listener.local_addr().expect("local addr");
        let store = Arc::new(Mutex::new(HashMap::new()));
        let served = Arc::clone(&store);
        thread::spawn(move || {
            for stream in listener.incoming().flatten() {
                Self::serve(stream, &served);
            }
        });
        Self { addr, store }
    }

    /// The proxy's base URL.
    #[must_use]
    pub fn url(&self) -> String {
        format!("http://{}", self.addr)
    }

    /// The certificate this fake gives `payload`.
    #[must_use]
    pub fn cert_of(payload: &[u8]) -> Bytes {
        let mut cert = vec![0x03];
        cert.extend_from_slice(keccak256(payload).as_slice());
        Bytes::from(cert)
    }

    /// The payloads stored so far, by certificate.
    ///
    /// # Panics
    /// Panics when the serving thread panicked while it held the store.
    #[must_use]
    pub fn stored(&self) -> HashMap<Bytes, Vec<u8>> {
        self.store.lock().expect("store lock").clone()
    }

    fn serve(mut stream: TcpStream, store: &Mutex<HashMap<Bytes, Vec<u8>>>) {
        let Some(request) = Request::read(&mut stream) else {
            return;
        };
        let (status, body) = Self::answer(&request, store);
        let head = format!(
            "HTTP/1.1 {status}\r\ncontent-type: application/octet-stream\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
            body.len()
        );
        let _ = stream.write_all(head.as_bytes());
        let _ = stream.write_all(&body);
    }

    fn answer(
        request: &Request,
        store: &Mutex<HashMap<Bytes, Vec<u8>>>,
    ) -> (&'static str, Vec<u8>) {
        let route = request.path.split('?').next().unwrap_or_default();
        match (request.method.as_str(), route.strip_prefix("/get/")) {
            ("POST", None) if route == "/put" => {
                let cert = Self::cert_of(&request.body);
                store
                    .lock()
                    .expect("store lock")
                    .insert(cert.clone(), request.body.clone());
                ("200 OK", cert.to_vec())
            }
            ("GET", Some(cert)) => {
                let cert = hex::decode(cert).map(Bytes::from);
                let found = cert
                    .ok()
                    .and_then(|c| store.lock().expect("store lock").get(&c).cloned());
                found.map_or(("404 Not Found", b"no such certificate".to_vec()), |p| {
                    ("200 OK", p)
                })
            }
            _ => ("404 Not Found", Vec::new()),
        }
    }
}
