//! Tiny RESP server for unit tests. It answers `CLIENT` with `+OK` and then
//! plays the scripted replies in order. A reply of `CLOSE` drops the socket.

use std::io::{Read, Write};
use std::net::{Shutdown, TcpListener};
use std::thread;
use std::time::Duration;

pub(crate) fn serve(replies: Vec<Vec<u8>>) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
    let address = listener.local_addr().expect("addr");
    thread::spawn(move || {
        let Ok((mut socket, _)) = listener.accept() else {
            return;
        };
        let _ = socket.set_read_timeout(Some(Duration::from_secs(2)));
        let _ = socket.set_write_timeout(Some(Duration::from_secs(2)));
        let mut buffer = Vec::new();
        let mut script = replies.into_iter();
        let mut chunk = [0u8; 4096];
        loop {
            while let Some(size) = value_len(&buffer) {
                let command = buffer[..size].to_vec();
                buffer.drain(..size);
                if command.windows(6).any(|window| window == b"CLIENT") {
                    if socket.write_all(b"+OK\r\n").is_err() {
                        return;
                    }
                    continue;
                }
                match script.next() {
                    Some(reply) if reply == b"CLOSE" => {
                        let _ = socket.shutdown(Shutdown::Both);
                        return;
                    }
                    Some(reply) => {
                        if socket.write_all(&reply).is_err() {
                            return;
                        }
                    }
                    None => {
                        if socket.write_all(b"+OK\r\n").is_err() {
                            return;
                        }
                    }
                }
            }
            match socket.read(&mut chunk) {
                Ok(0) | Err(_) => return,
                Ok(read) => buffer.extend_from_slice(&chunk[..read]),
            }
        }
    });
    format!("redis://{address}")
}

pub(crate) fn ok() -> Vec<u8> {
    b"+OK\r\n".to_vec()
}

pub(crate) fn queued() -> Vec<u8> {
    b"+QUEUED\r\n".to_vec()
}

pub(crate) fn error(text: &str) -> Vec<u8> {
    format!("-{text}\r\n").into_bytes()
}

pub(crate) fn integer(number: i64) -> Vec<u8> {
    format!(":{number}\r\n").into_bytes()
}

pub(crate) fn nil() -> Vec<u8> {
    b"*-1\r\n".to_vec()
}

pub(crate) fn empty_array() -> Vec<u8> {
    b"*0\r\n".to_vec()
}

pub(crate) fn bulk(text: &str) -> Vec<u8> {
    format!("${}\r\n{text}\r\n", text.len()).into_bytes()
}

pub(crate) fn array(parts: &[Vec<u8>]) -> Vec<u8> {
    let mut out = format!("*{}\r\n", parts.len()).into_bytes();
    for part in parts {
        out.extend_from_slice(part);
    }
    out
}

pub(crate) fn time_reply() -> Vec<u8> {
    array(&[integer(10), integer(20)])
}

fn value_len(buffer: &[u8]) -> Option<usize> {
    if buffer.is_empty() {
        return None;
    }
    match buffer[0] {
        b'+' | b'-' | b':' => crlf(buffer, 1),
        b'$' => {
            let line = crlf(buffer, 1)?;
            let length: isize = std::str::from_utf8(&buffer[1..line - 2]).ok()?.parse().ok()?;
            if length < 0 {
                return Some(line);
            }
            let end = line + length as usize + 2;
            if buffer.len() >= end {
                Some(end)
            } else {
                None
            }
        }
        b'*' => {
            let line = crlf(buffer, 1)?;
            let count: isize = std::str::from_utf8(&buffer[1..line - 2]).ok()?.parse().ok()?;
            if count < 0 {
                return Some(line);
            }
            let mut position = line;
            for _ in 0..count {
                position += value_len(&buffer[position..])?;
            }
            Some(position)
        }
        _ => None,
    }
}

fn crlf(buffer: &[u8], from: usize) -> Option<usize> {
    buffer[from..].windows(2).position(|window| window == b"\r\n").map(|position| from + position + 2)
}

#[cfg(test)]
mod tests {
    use super::value_len;

    #[test]
    fn every_prefix_has_a_length() {
        assert_eq!(value_len(b"+OK\r\n"), Some(5));
        assert_eq!(value_len(b"-ERR\r\n"), Some(6));
        assert_eq!(value_len(b":1\r\n"), Some(4));
        assert_eq!(value_len(b"$-1\r\n"), Some(5));
        assert_eq!(value_len(b"$3\r\n"), None);
        assert_eq!(value_len(b"*-1\r\n"), Some(5));
        assert_eq!(value_len(b"x"), None);
        assert_eq!(value_len(b""), None);
    }
}
