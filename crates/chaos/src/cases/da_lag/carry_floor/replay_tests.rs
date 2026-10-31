use super::*;

#[test]
fn payload_limits_exclude_the_http_length_prefixes() {
    let bytes = [
        2u32.to_le_bytes().as_slice(),
        &[1, 2],
        3u32.to_le_bytes().as_slice(),
        &[3, 4, 5],
    ]
    .concat();
    let cursor = Cursor {
        index: 10,
        block: 20,
    };
    let page = Page {
        bytes,
        after: cursor,
        head: cursor,
    };
    assert_eq!(page.payload_bytes().unwrap(), 5);
}

#[test]
fn empty_and_truncated_ranges_do_not_certify_replay() {
    for bytes in [
        vec![],
        vec![1, 2],
        [10u32.to_le_bytes().as_slice(), &[1, 2]].concat(),
    ] {
        let cursor = Cursor {
            index: 10,
            block: 20,
        };
        let page = Page {
            bytes,
            after: cursor,
            head: cursor,
        };
        assert!(page.payload_bytes().is_err());
    }
}

#[test]
fn both_cursor_components_must_reach_the_target() {
    let target = Cursor {
        index: 10,
        block: 20,
    };
    assert!(
        !Cursor {
            index: 11,
            block: 19
        }
        .covers(target)
    );
    assert!(
        !Cursor {
            index: 9,
            block: 21
        }
        .covers(target)
    );
    assert!(target.covers(target));
}

struct Reply {
    status: &'static str,
    bytes: Vec<u8>,
}

impl Reply {
    async fn serve(self) -> (String, tokio::task::JoinHandle<()>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let task = tokio::spawn(async move {
            use tokio::io::{AsyncReadExt, AsyncWriteExt};

            let (mut stream, _) = listener.accept().await.unwrap();
            let mut request = vec![0; 4096];
            let len = stream.read(&mut request).await.unwrap();
            let request = std::str::from_utf8(&request[..len]).unwrap();
            assert!(request.contains("from_index=10&from_block=20&limit="));
            let headers = format!(
                "HTTP/1.1 {}\r\nContent-Length: {}\r\nx-egress-after-index: 12\r\nx-egress-after-block: 22\r\nx-egress-next-index: 12\r\nx-egress-next-block: 22\r\nConnection: close\r\n\r\n",
                self.status,
                self.bytes.len()
            );
            stream.write_all(headers.as_bytes()).await.unwrap();
            stream.write_all(&self.bytes).await.unwrap();
        });
        (url, task)
    }

    async fn compare(self) -> anyhow::Result<Cursor> {
        let canonical = Self {
            status: "200 OK",
            bytes: [2u32.to_le_bytes().as_slice(), &[1, 2]].concat(),
        };
        let (first, first_task) = canonical.serve().await;
        let (second, second_task) = self.serve().await;
        let copies = Copies {
            http: reqwest::Client::builder()
                .timeout(Duration::from_secs(1))
                .build()?,
            first,
            rest: vec![second],
        };
        let verdict = copies
            .replay(
                Cursor {
                    index: 10,
                    block: 20,
                },
                0,
            )
            .await;
        first_task.await.unwrap();
        second_task.await.unwrap();
        verdict
    }
}

#[tokio::test]
async fn identical_http_ranges_pass_but_divergent_bytes_and_a_lost_floor_fail() {
    assert!(
        Reply {
            status: "200 OK",
            bytes: [2u32.to_le_bytes().as_slice(), &[1, 2]].concat(),
        }
        .compare()
        .await
        .is_ok()
    );
    assert!(
        Reply {
            status: "200 OK",
            bytes: [2u32.to_le_bytes().as_slice(), &[1, 3]].concat(),
        }
        .compare()
        .await
        .is_err()
    );
    assert!(
        Reply {
            status: "410 Gone",
            bytes: vec![],
        }
        .compare()
        .await
        .is_err()
    );
}
