use std::str::{from_utf8, FromStr};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
// Uncomment this block to pass the first stage
use tokio::net::TcpListener;

#[derive(Debug, Clone)]
pub enum RedisMessage {
    Array(RedisArrayMessage),
    BulkString(RedisBulkStringMessage),
}

impl RedisMessage {
    fn to_message(&self) -> String {
        match self {
            RedisMessage::Array(array) => {
                array.to_message()
            }
            RedisMessage::BulkString(bulk) => {
                bulk.to_message()
            }
        }
    }
    fn parse_slice(slice: &[u8], start: usize) -> Result<(Self, usize), String> {
        let symbol = slice.get(start).ok_or(String::new())?;
        // println!("parsed {}", *symbol as char);
        // println!("Starting to parse {}", from_utf8(&slice[start + 1..start + 3]).unwrap());
        match *symbol as char {
            '*' => {
                let (array, end) = RedisArrayMessage::parse_slice(slice, start + 1)?;
                Ok((Self::Array(array), end))
            }
            '$' => {
                let (string, end) = RedisBulkStringMessage::parse_slice(slice, start + 1)?;
                // println!("Parsed string {string:?}");
                Ok((Self::BulkString(string), end))
            }
            _ => {
                Err(format!("unrecognised symbol {symbol}"))
            }
        }
    }
}

fn find_r(slice: &[u8], start: usize) -> Result<usize, String> {
    // println!("Find r called {start}");
    let mut number_end = start;

    while slice[number_end] as char != '\r' {
        println!("{}", slice[number_end] as char);
        number_end += 1;
    }
    assert_eq!(slice[number_end + 1] as char, '\n');
    Ok(number_end)
}

fn parse_length(slice: &[u8], start: usize) -> Result<(usize, usize), String> {
    let number_end = find_r(slice, start)?;
    let size = usize::from_str(from_utf8(&slice[start..number_end]).unwrap()).unwrap();
    Ok((size, number_end + 2))
}

#[derive(Debug, Clone)]
pub struct RedisBulkStringMessage {
    pub content: String,
}
impl From<String> for RedisBulkStringMessage {
    fn from(content: String) -> Self {
        Self{ content }
    }
}
impl RedisBulkStringMessage {
    fn to_message(&self) -> String {
        let len = self.content.len();
        format!("${len}\r\n{}\r\n",self.content)
    }
    fn parse_slice(slice: &[u8], start: usize) -> Result<(Self, usize), String> {
        let (size, start) = parse_length(slice, start).unwrap();
        let string_end = start + size;
        let content = from_utf8(&slice[start..string_end]).unwrap().to_string();
        Ok((Self { content }, string_end + 2))
    }
}

#[derive(Debug, Clone)]
pub struct RedisArrayMessage {
    pub messages: Vec<RedisMessage>,
}

impl RedisArrayMessage {
    fn to_message(&self) -> String {

        todo!()
    }
    fn parse_slice(slice: &[u8], start: usize) -> Result<(Self, usize), String> {
        let (size, mut start) = parse_length(slice, start).unwrap();
        let mut messages = Vec::with_capacity(size);
        // println!("size {size}");
        for _ in 0..size {
            let (msg, start_new) = RedisMessage::parse_slice(slice, start).unwrap();
            start = start_new;
            messages.push(msg)
        }
        Ok((Self { messages }, start))
    }
}

#[tokio::main]
async fn main() {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");

    // Uncomment this block to pass the first stage
    //
    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();
    //
    let mut buf = [0; 512];
    loop {
        let stream = listener.accept().await;
        match stream {
            Ok((mut stream, _)) => {
                println!("accepted new connection");
                tokio::spawn(async move {
                    while let Ok(size) = stream.read(&mut buf).await {
                        // println!("{}", from_utf8(&buf[..size]).unwrap());
                        let (rcvd_msg, _) = RedisMessage::parse_slice(&buf[..size], 0).unwrap();
                        println!("{rcvd_msg:?}");
                        if let RedisMessage::Array(array) = rcvd_msg {
                            if let Some(RedisMessage::BulkString(RedisBulkStringMessage { content })) = array.messages.first() {
                                if content.to_uppercase().as_str() == "ECHO" {
                                    if let Some(RedisMessage::BulkString(RedisBulkStringMessage { content })) = array.messages.get(1) {
                                        let reply = RedisMessage::BulkString(content.clone().into()).to_message();
                                        println!("Sending message {reply}");
                                        stream.write_all(reply.as_bytes()).await.unwrap();
                                        continue;
                                    }
                                }
                            }
                        }
                        // let rcved = from_utf8(&buf[..size]).unwrap();
                        // println!("Received {rcved}");
                        let msg = "+PONG\r\n".as_bytes();
                        stream.write_all(msg).await.unwrap();
                    }
                });
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}
