//! A file-backed stand-in for the EigenDA proxy, for a deployment without
//! EigenDA: the local profile and the e2e suites, as anvil stands in for
//! L1.
//!
//! It serves the proxy's API: `POST /put` stores the payload and answers
//! with a certificate, `GET /get/<hex cert>` answers with the payload,
//! `GET /health` answers once it listens. A payload lives in a file
//! under the store's directory, so it survives a restart the way EigenDA
//! survives a proxy restart; the proxy's own in-memory store does not,
//! and a rebuild after a node outage found nothing there.
//!
//! The certificate is `0x03` (the current version byte) followed by the
//! payload's keccak; a real one is an RLP body of about 400 bytes that
//! the verifier contract understands. The real proxy answers an unknown
//! certificate with 500; this store answers 404. The client treats every
//! non-success alike.

use std::fs;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::thread;

use alloy_primitives::{Bytes, hex, keccak256};

use crate::error::BatcherError;

/// The store: one directory of payloads, named by the certificate's hex.
#[derive(Clone, Debug)]
pub struct DaStore {
    dir: PathBuf,
}

/// One HTTP request, as far as the store reads it.
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

impl DaStore {
    /// Open the store at `dir`; create it.
    ///
    /// # Errors
    /// Returns an error when the directory cannot be created.
    pub fn open(dir: impl AsRef<Path>) -> Result<Self, BatcherError> {
        let dir = dir.as_ref().to_path_buf();
        fs::create_dir_all(&dir)?;
        Ok(Self { dir })
    }

    /// The certificate this store gives `payload`.
    #[must_use]
    pub fn cert_of(payload: &[u8]) -> Bytes {
        let mut cert = vec![0x03];
        cert.extend_from_slice(keccak256(payload).as_slice());
        Bytes::from(cert)
    }

    fn path_of(&self, da_cert: &Bytes) -> PathBuf {
        self.dir.join(format!("{}.bin", hex::encode(da_cert)))
    }

    /// Store `payload`; its certificate.
    ///
    /// # Errors
    /// Returns an error when the write fails.
    pub fn put(&self, payload: &[u8]) -> Result<Bytes, BatcherError> {
        let cert = Self::cert_of(payload);
        let path = self.path_of(&cert);
        let tmp = path.with_extension("tmp");
        fs::write(&tmp, payload)?;
        fs::rename(&tmp, &path)?;
        Ok(cert)
    }

    /// The payload under `da_cert`, or `None`.
    ///
    /// # Errors
    /// Returns an error when the read fails.
    pub fn get(&self, da_cert: &Bytes) -> Result<Option<Vec<u8>>, BatcherError> {
        let path = self.path_of(da_cert);
        if !path.exists() {
            return Ok(None);
        }
        Ok(Some(fs::read(path)?))
    }

    /// Bind `addr` and serve the proxy's API on it, one thread per
    /// connection, until the process ends. Returns the bound address.
    ///
    /// # Errors
    /// Returns an error when the bind fails.
    pub fn serve(self, addr: SocketAddr) -> Result<SocketAddr, BatcherError> {
        let listener = TcpListener::bind(addr)?;
        let local = listener.local_addr()?;
        thread::spawn(move || {
            for stream in listener.incoming().flatten() {
                let store = self.clone();
                thread::spawn(move || store.handle(stream));
            }
        });
        Ok(local)
    }

    fn handle(&self, mut stream: TcpStream) {
        let Some(request) = Request::read(&mut stream) else {
            return;
        };
        let (status, body) = self.answer(&request);
        let head = format!(
            "HTTP/1.1 {status}\r\ncontent-type: application/octet-stream\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
            body.len()
        );
        let _ = stream.write_all(head.as_bytes());
        let _ = stream.write_all(&body);
    }

    fn answer(&self, request: &Request) -> (&'static str, Vec<u8>) {
        let route = request.path.split('?').next().unwrap_or_default();
        match (request.method.as_str(), route.strip_prefix("/get/")) {
            ("POST", None) if route == "/put" => match self.put(&request.body) {
                Ok(cert) => ("200 OK", cert.to_vec()),
                Err(e) => ("500 Internal Server Error", e.to_string().into_bytes()),
            },
            ("GET", None) if route == "/health" => ("200 OK", Vec::new()),
            ("GET", Some(cert)) => {
                let cert = hex::decode(cert).map(Bytes::from);
                match cert.ok().map(|c| self.get(&c)) {
                    Some(Ok(Some(payload))) => ("200 OK", payload),
                    Some(Err(e)) => ("500 Internal Server Error", e.to_string().into_bytes()),
                    _ => ("404 Not Found", b"no such certificate".to_vec()),
                }
            }
            _ => ("404 Not Found", Vec::new()),
        }
    }
}
