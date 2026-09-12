use rhizadb::Db;
use serde_json::{json, Value};
use std::{
    env,
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    sync::Arc,
    thread,
    time::Duration,
};

// Test-only chaos fixture; this intentionally minimal HTTP parser is not production code.
const MAX_BODY: usize = 64 << 10;
const MAX_HEADERS: usize = 16 << 10;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut db = Db::open_from_env()?;
    db.start_operator(&env::var("RHIZA_BIND_ADDR").unwrap_or_else(|_| "0.0.0.0:9091".into()))?;
    let listener = TcpListener::bind("0.0.0.0:8080")?;
    let db = Arc::new(db);
    for stream in listener.incoming() {
        let db = db.clone();
        if let Ok(stream) = stream {
            thread::spawn(move || serve(stream, db));
        }
    }
    Ok(())
}

fn serve(mut stream: TcpStream, db: Arc<Db>) {
    let _ = stream.set_read_timeout(Some(Duration::from_secs(5)));
    let _ = stream.set_write_timeout(Some(Duration::from_secs(5)));
    let result = request(&mut stream).and_then(|(method, path, body)| {
        match (method.as_str(), path.as_str()) {
            ("GET", "/host") => Ok((200, json!({"host":"rust-embedded"}))),
            ("GET", "/healthz") => Ok((200, json!({}))),
            ("GET", "/ready") => Ok((
                if db.ready().map_err(|e| e.to_string())? {
                    200
                } else {
                    503
                },
                json!({}),
            )),
            ("POST", "/sql/execute") => Ok((
                200,
                db.call::<Value>("execute", body)
                    .map_err(|e| e.to_string())?,
            )),
            ("POST", "/sql/query") => Ok((
                200,
                db.call::<Value>("query", body).map_err(|e| e.to_string())?,
            )),
            ("GET", _) | ("POST", _) => Ok((404, json!({"error":"not found"}))),
            _ => Ok((405, json!({"error":"method not allowed"}))),
        }
    });
    let (status, body) = result.unwrap_or_else(|error| (400, json!({"error":error})));
    let bytes = body.to_string();
    let reason = if status == 200 { "OK" } else { "Error" };
    let _ = write!(stream, "HTTP/1.1 {status} {reason}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}", bytes.len(), bytes);
}

fn request(stream: &mut TcpStream) -> Result<(String, String, Value), String> {
    let mut raw = Vec::new();
    let mut chunk = [0; 1024];
    let end = loop {
        if raw.len() > MAX_HEADERS + MAX_BODY {
            return Err("request too large".into());
        }
        let read = stream.read(&mut chunk).map_err(|e| e.to_string())?;
        if read == 0 {
            return Err("incomplete request".into());
        }
        raw.extend_from_slice(&chunk[..read]);
        if let Some(end) = raw.windows(4).position(|v| v == b"\r\n\r\n") {
            if end + 4 > MAX_HEADERS {
                return Err("headers too large".into());
            }
            break end + 4;
        }
        if raw.len() > MAX_HEADERS {
            return Err("headers too large".into());
        }
    };
    let headers = std::str::from_utf8(&raw[..end]).map_err(|_| "invalid headers")?;
    let mut lines = headers.split("\r\n");
    let first = lines.next().ok_or("missing request line")?;
    let mut first = first.split_whitespace();
    let method = first.next().ok_or("missing method")?.to_owned();
    let path = first
        .next()
        .ok_or("missing path")?
        .split('?')
        .next()
        .unwrap()
        .to_owned();
    if first.next() != Some("HTTP/1.1") || first.next().is_some() {
        return Err("invalid request line".into());
    }
    let mut length = None;
    for line in lines {
        if line.is_empty() {
            continue;
        }
        let Some((name, value)) = line.split_once(':') else {
            return Err("malformed header".into());
        };
        if name.eq_ignore_ascii_case("transfer-encoding") {
            return Err("transfer encoding unsupported".into());
        }
        if name.eq_ignore_ascii_case("content-length")
            && length
                .replace(value.trim().parse().map_err(|_| "invalid content length")?)
                .is_some()
        {
            return Err("duplicate content length".into());
        }
    }
    let length = length.unwrap_or(0);
    if length > MAX_BODY {
        return Err("body too large".into());
    }
    while raw.len() - end < length {
        let read = stream.read(&mut chunk).map_err(|e| e.to_string())?;
        if read == 0 || raw.len() + read > end + length {
            return Err("invalid body length".into());
        }
        raw.extend_from_slice(&chunk[..read]);
    }
    let body = if length == 0 {
        json!({})
    } else {
        serde_json::from_slice(&raw[end..end + length]).map_err(|_| "invalid json")?
    };
    Ok((method, path, body))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(raw: String) -> Result<(String, String, Value), String> {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let client = thread::spawn(move || {
            let mut client = TcpStream::connect(address).unwrap();
            let _ = client.write_all(raw.as_bytes());
        });
        let (mut stream, _) = listener.accept().unwrap();
        let result = request(&mut stream);
        client.join().unwrap();
        result
    }

    #[test]
    fn parser_bounds_and_validates_framing() {
        let oversized = format!("GET / HTTP/1.1\r\nX: {}\r\n\r\n", "x".repeat(MAX_HEADERS));
        let cases = [
            ("get", "GET /host HTTP/1.1\r\n\r\n".to_owned(), true),
            (
                "post",
                "POST /sql/query HTTP/1.1\r\ncOnTeNt-LeNgTh: 2\r\n\r\n{}".to_owned(),
                true,
            ),
            (
                "bad length",
                "POST / HTTP/1.1\r\nContent-Length: x\r\n\r\n".to_owned(),
                false,
            ),
            (
                "duplicate length",
                "POST / HTTP/1.1\r\nContent-Length: 0\r\nContent-Length: 0\r\n\r\n".to_owned(),
                false,
            ),
            (
                "transfer encoding",
                "POST / HTTP/1.1\r\nTransfer-Encoding: chunked\r\n\r\n".to_owned(),
                false,
            ),
            ("oversized header", oversized, false),
            (
                "malformed header",
                "GET / HTTP/1.1\r\nBroken\r\n\r\n".to_owned(),
                false,
            ),
            ("missing version", "GET /\r\n\r\n".to_owned(), false),
            ("wrong version", "GET / HTTP/2.0\r\n\r\n".to_owned(), false),
            (
                "extra request token",
                "GET / HTTP/1.1 extra\r\n\r\n".to_owned(),
                false,
            ),
        ];
        for (name, raw, valid) in cases {
            assert_eq!(parse(raw).is_ok(), valid, "{name}");
        }
    }
}
