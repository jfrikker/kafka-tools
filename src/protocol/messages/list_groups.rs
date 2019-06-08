use super::super::request::*;

#[derive(Debug, Clone)]
pub struct Request { }

impl KafkaSerializable for Request {
    fn serialize<W: Write>(&self, _: &mut W)-> Result<()> {
        Ok(())
    }
}

impl KafkaRequest for Request {
    type Response = Response;

    fn api_key() -> i16 {
        16
    }

    fn api_version() -> i16 {
        2
    }
}

#[derive(Debug, Clone)]
pub struct Response {
    pub throttle_time_ms: i32,
    pub error_code: i16,
    pub groups: Vec<Group>
}

impl KafkaDeserializable for Response {
    fn deserialize<R: Read>(stream: &mut R) -> Result<Self> {
        let throttle_time_ms = i32::deserialize(stream)?;
        let error_code = i16::deserialize(stream)?;
        let groups = Vec::<Group>::deserialize(stream)?;
        Ok(Response {
            throttle_time_ms,
            error_code,
            groups
        })
    }
}

#[derive(Debug, Clone)]
pub struct Group {
    pub name: String,
    pub protocol_type: String
}

impl KafkaDeserializable for Group {
    fn deserialize<R: Read>(stream: &mut R) -> Result<Self> {
        let name = String::deserialize(stream)?;
        let protocol_type = String::deserialize(stream)?;
        Ok(Group {
            name,
            protocol_type
        })
    }
}