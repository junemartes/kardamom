//! RESP command decoding shared by controlled Redis and Sentinel peers.

use tokio::io::{AsyncBufReadExt, AsyncReadExt, BufReader};
use tokio::net::TcpStream;

pub(super) struct Commands;

impl Commands {
    pub(super) async fn read(stream: &mut BufReader<TcpStream>) -> Option<Vec<String>> {
        let mut line = String::new();
        if stream.read_line(&mut line).await.ok()? == 0 {
            return None;
        }
        let count: usize = line.trim().strip_prefix('*')?.parse().ok()?;
        let mut command = Vec::new();
        for _ in 0..count {
            command.push(Self::bulk(stream).await?);
        }
        Some(command)
    }

    async fn bulk(stream: &mut BufReader<TcpStream>) -> Option<String> {
        let mut line = String::new();
        stream.read_line(&mut line).await.ok()?;
        let len: usize = line.trim().strip_prefix('$')?.parse().ok()?;
        let mut bytes = vec![0; len.checked_add(2)?];
        stream.read_exact(&mut bytes).await.ok()?;
        String::from_utf8(bytes[..len].to_vec()).ok()
    }
}
