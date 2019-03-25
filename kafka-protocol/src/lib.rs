extern crate byteorder;
extern crate bufstream;

pub mod messages;
pub mod request;

use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use request::{KafkaDeserializable, KafkaRequest, KafkaSerializable};
use std::io::{Cursor, Result, Write};
use std::net::{TcpStream, ToSocketAddrs};

pub struct KafkaConnection {
    conn: TcpStream,
    outgoing: Cursor<Vec<u8>>
}

impl KafkaConnection {
    pub fn connect<A: ToSocketAddrs>(addr: A) -> Result<KafkaConnection> {
        TcpStream::connect(addr).map(KafkaConnection::with_stream)
    }

    pub fn with_stream(conn: TcpStream) -> KafkaConnection {
        KafkaConnection {
            conn,
            outgoing: Cursor::new(vec!(0; 2 * 1024 * 1024))
        }
    }

    pub fn send <R: KafkaRequest>(&mut self, request: &R) -> Result<R::Response> {
        self.send_req(request)?;
        self.read_resp()
    }

    fn send_req <R: KafkaRequest>(&mut self, request: &R) -> Result<()> {
        self.outgoing.set_position(0);
        self.outgoing.write_i16::<BigEndian>(R::api_key())?;
        self.outgoing.write_i16::<BigEndian>(R::api_version())?;
        self.outgoing.write_i32::<BigEndian>(0)?;
        self.outgoing.write_i16::<BigEndian>(-1)?;
        request.serialize(&mut self.outgoing)?;
        let size = self.outgoing.position() as usize;
        (size as i32).serialize(&mut self.conn)?;
        self.conn.write_all(&self.outgoing.get_ref()[0..size])?;
        self.conn.flush()?;
        Ok(())
    }

    fn read_resp <R: KafkaDeserializable>(&mut self) -> Result<R> {
        self.conn.read_i32::<BigEndian>()?;
        self.conn.read_i32::<BigEndian>()?;
        R::deserialize(&mut self.conn)
    }
}