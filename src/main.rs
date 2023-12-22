use tokio::io::{AsyncReadExt, AsyncWriteExt};
// Uncomment this block to pass the first stage
use tokio::net::TcpListener;

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
            Ok( (mut stream,_)) => {
                println!("accepted new connection");
                tokio::spawn(async move {
                    while let Ok(_size) = stream.read(&mut buf).await {
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
