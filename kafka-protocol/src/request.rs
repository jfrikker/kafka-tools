pub use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
pub use std::io::{Read, Result, Write};

pub trait KafkaSerializable {
    fn serialize<W: Write>(&self, out: &mut W)-> Result<()>;
}

pub trait KafkaDeserializable: Sized {
    fn deserialize<R: Read>(stream: &mut R) -> Result<Self>;
}

impl KafkaSerializable for bool {
    fn serialize<W: Write>(&self, out: &mut W) -> Result<()> {
        if *self {
            out.write_all(&[0])?;
        } else {
            out.write_all(&[1])?;
        }
        Ok(())
    }
}

impl KafkaDeserializable for bool {
    fn deserialize<R: Read>(stream: &mut R) -> Result<Self> {
        let mut data = [0 as u8];
        stream.read_exact(&mut data)?;
        Ok(data[0] != 0)
    }
}

impl KafkaSerializable for i8 {
    fn serialize<W: Write>(&self, out: &mut W) -> Result<()> {
        out.write_i8(*self)
    }
}

impl KafkaSerializable for i16 {
    fn serialize<W: Write>(&self, out: &mut W) -> Result<()> {
        out.write_i16::<BigEndian>(*self)
    }
}

impl KafkaDeserializable for i16 {
    fn deserialize<R: Read>(stream: &mut R) -> Result<Self> {
        stream.read_i16::<BigEndian>()
    }
}

impl KafkaSerializable for i32 {
    fn serialize<W: Write>(&self, out: &mut W) -> Result<()> {
        out.write_i32::<BigEndian>(*self)
    }
}

impl KafkaDeserializable for i32 {
    fn deserialize<R: Read>(stream: &mut R) -> Result<Self> {
        stream.read_i32::<BigEndian>()
    }
}

impl KafkaSerializable for i64 {
    fn serialize<W: Write>(&self, out: &mut W) -> Result<()> {
        out.write_i64::<BigEndian>(*self)
    }
}

impl KafkaDeserializable for i64 {
    fn deserialize<R: Read>(stream: &mut R) -> Result<Self> {
        stream.read_i64::<BigEndian>()
    }
}

impl KafkaSerializable for String {
    fn serialize<W: Write>(&self, out: &mut W) -> Result<()> {
        (self.len() as i16).serialize(out)?;
        write!(out, "{}", self)
    }
}

impl KafkaDeserializable for String {
    fn deserialize<R: Read>(stream: &mut R) -> Result<Self> {
        let len = i16::deserialize(stream)?;
        let mut data = vec!(0; len as usize);
        stream.read_exact(&mut data)?;
        Ok(String::from_utf8(data).unwrap())
    }
}

impl KafkaDeserializable for Option<String> {
    fn deserialize<R: Read>(stream: &mut R) -> Result<Self> {
        let len = i16::deserialize(stream)?;
        if len == -1 {
            Ok(None)
        } else {
            let mut data = vec!(0; len as usize);
            stream.read_exact(&mut data)?;
            Ok(Some(String::from_utf8(data).unwrap()))
        }
    }
}

impl <T: KafkaSerializable> KafkaSerializable for Vec<T> {
    fn serialize<W: Write>(&self, out: &mut W) -> Result<()> {
        (self.len() as i32).serialize(out)?;
        for i in self.iter() {
            i.serialize(out)?;
        }
        Ok(())
    }
}

impl <T: KafkaDeserializable> KafkaDeserializable for Vec<T> {
    fn deserialize<R: Read>(stream: &mut R) -> Result<Self> {
        let len = i32::deserialize(stream)?;
        let mut result = Vec::new();
        for _ in 0..len {
            result.push(T::deserialize(stream)?);
        }
        Ok(result)
    }
}

impl <T: KafkaSerializable> KafkaSerializable for Option<Vec<T>> {
    fn serialize<W: Write>(&self, out: &mut W) -> Result<()> {
        match self {
            None => (-1 as i32).serialize(out),
            Some(v) => v.serialize(out)
        }
    }
}

pub trait KafkaRequest: KafkaSerializable {
    type Response: KafkaDeserializable;
    fn api_key() -> i16;
    fn api_version() -> i16;
}