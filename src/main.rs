use bytes::{BufMut, BytesMut};
use cached::OpCode;
use tokio::net::tcp::{ReadHalf, WriteHalf};
use tokio::net::TcpStream;

#[tokio::main]
async fn main() {
    let mut stream = TcpStream::connect("127.0.0.1:7878").await.unwrap();
    let (rx, tx) = stream.split();

    tx.writable().await.unwrap();
    write_stream(&tx);

    rx.readable().await.unwrap();
    read_stream(&rx);

    // let listener = TcpListener::bind("127.0.0.1:7878").await.unwrap();
    //
    // loop {
    //     let (stream, _) = listener.accept().await.unwrap();
    //     stream.readable().await.unwrap();
    //     read_stream(&stream);
    //     stream.writable().await.unwrap();
    //     write_stream(&stream);
    // }
}

fn read_stream(stream: &ReadHalf) {
    let mut buffer: [u8; 1024] = [0; 1024];
    let length = match stream.try_read(&mut buffer[..]) {
        Ok(0) => return,
        Err(_) => return,
        Ok(n) => n,
    };
    println!("GOT: {}", String::from_utf8_lossy(&buffer[..length]));
}

fn write_stream(stream: &WriteHalf) {
    // header SET; keylen; total len
    // key
    // value
    //
    let op = OpCode::Set;
    let key = b"hello";
    let value = b"world!";
    let header_size_bytes: u8 = 6;
    let key_size: u8 = key.len() as u8;
    let value_size: u8 = value.len() as u8;
    let payload_size: u32 = header_size_bytes as u32 + key_size as u32 + value_size as u32;
    let mut buf = BytesMut::with_capacity(payload_size as usize);
    buf.put_u8(op as u8);
    buf.put_u8(key_size);
    buf.put_u32(payload_size);
    buf.put(&key[..]);
    buf.put(&value[..]);
    let bytes = buf.freeze();
    println!("bytes: {:?}", bytes);
    stream.try_write(bytes.as_ref()).unwrap();
}
