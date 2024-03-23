pub trait PrimitiveParser<T> {
    fn parse(bytes: &[u8]) -> Option<T>;
}

pub trait PrimitiveWriter<T> {
    fn write(value: T) -> Vec<u8>;
}

pub struct Float32Type{
    pub value: Option<f32>,
}

pub struct Float64Type{
    pub value: Option<f64>,

}
pub struct Int16Type{
    pub value: Option<i16>,
}
pub struct Int32Type{
    pub value: Option<i32>,
}
pub struct Int64Type{
    pub value: Option<i64>,
}
pub struct Int8Type{
    pub value: Option<i8>,
}
pub struct UInt16Type{
    pub value: Option<u16>,
}
pub struct UInt32Type{
    pub value: Option<u32>,
}
pub struct UInt64Type{
    pub value: Option<u64>,
}
pub struct UInt8Type{
    pub value: Option<u8>,
}
pub struct BooleanType{
    pub value: Option<bool>,
}
pub struct Utf8Type{
    pub value: Option<String>,
}
pub struct ListType{
    pub value: Option<Vec<String>>,
}

impl PrimitiveParser<f32> for Float32Type {
    fn parse(bytes: &[u8]) -> Option<f32> {
        // TODO: Maybe test with byteorder::FromBigEndian
        Some(f32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
    }
}

impl PrimitiveParser<f64> for Float64Type {
    fn parse(bytes: &[u8]) -> Option<f64> {
        Some(f64::from_be_bytes(
            [bytes[0], bytes[1], bytes[2], bytes[3], 
            bytes[4], bytes[5], bytes[6], bytes[7]]
        ))
    }
}

impl PrimitiveParser<i16> for Int16Type {
    fn parse(bytes: &[u8]) -> Option<i16> {
        Some(i16::from_be_bytes([bytes[0], bytes[1]]))
    }
}

impl PrimitiveParser<i32> for Int32Type {
    fn parse(bytes: &[u8]) -> Option<i32> {
        Some(i32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
    }
}

impl PrimitiveParser<i64> for Int64Type {
    fn parse(bytes: &[u8]) -> Option<i64> {
        Some(i64::from_be_bytes(
            [bytes[0], bytes[1], bytes[2], bytes[3], 
            bytes[4], bytes[5], bytes[6], bytes[7]]
        ))
    }
}

impl PrimitiveParser<i8> for Int8Type {
    fn parse(bytes: &[u8]) -> Option<i8> {
        Some(i8::from_be_bytes([bytes[0]]))
    }
}

impl PrimitiveParser<u16> for UInt16Type {
    fn parse(bytes: &[u8]) -> Option<u16> {
        Some(u16::from_be_bytes([bytes[0], bytes[1]]))
    }
}

impl PrimitiveParser<u32> for UInt32Type {
    fn parse(bytes: &[u8]) -> Option<u32> {
        Some(u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
    }
}

impl PrimitiveParser<u64> for UInt64Type {
    fn parse(bytes: &[u8]) -> Option<u64> {
        Some(u64::from_be_bytes(
            [bytes[0], bytes[1], bytes[2], bytes[3], 
            bytes[4], bytes[5], bytes[6], bytes[7]]
        ))
    }
}

impl PrimitiveParser<u8> for UInt8Type {
    fn parse(bytes: &[u8]) -> Option<u8> {
        Some(u8::from_be_bytes([bytes[0]]))
    }
}

impl PrimitiveParser<bool> for BooleanType {
    fn parse(bytes: &[u8]) -> Option<bool> {
        Some(bytes[0] != 0)
    }
}

impl PrimitiveParser<String> for Utf8Type {
    fn parse(bytes: &[u8]) -> Option<String> {
        let value = unsafe { std::str::from_utf8_unchecked(bytes) };
        Some(value.to_string())
    }
}   

impl PrimitiveParser<Vec<String>> for ListType {
    fn parse(bytes: &[u8]) -> Option<Vec<String>> {
        let mut values = Vec::new();
        let mut i = 0;
        while i < bytes.len() {
            let len = f32::from_be_bytes([bytes[i], bytes[i+1], bytes[i+2], bytes[i+3]]) as usize;
            let value = unsafe { std::str::from_utf8_unchecked(&bytes[i+4..i+4+len]) };
            values.push(value.to_string());
            i += 4 + len;
        }
        Some(values)
    }
}

impl PrimitiveWriter<f32> for Float32Type {
    fn write(value: f32) -> Vec<u8> {
        value.to_be_bytes().to_vec()
    }
}

impl PrimitiveWriter<f64> for Float64Type {
    fn write(value: f64) -> Vec<u8> {
        value.to_be_bytes().to_vec()
    }
}

impl PrimitiveWriter<i16> for Int16Type {
    fn write(value: i16) -> Vec<u8> {
        value.to_be_bytes().to_vec()
    }
}

impl PrimitiveWriter<i32> for Int32Type {
    fn write(value: i32) -> Vec<u8> {
        value.to_be_bytes().to_vec()
    }
}   

impl PrimitiveWriter<i64> for Int64Type {
    fn write(value: i64) -> Vec<u8> {
        value.to_be_bytes().to_vec()
    }
}       

impl PrimitiveWriter<i8> for Int8Type {
    fn write(value: i8) -> Vec<u8> {
        value.to_be_bytes().to_vec()
    }
}

impl PrimitiveWriter<u16> for UInt16Type {
    fn write(value: u16) -> Vec<u8> {
        value.to_be_bytes().to_vec()
    }
}

impl PrimitiveWriter<u32> for UInt32Type {
    fn write(value: u32) -> Vec<u8> {
        value.to_be_bytes().to_vec()
    }
}

impl PrimitiveWriter<u64> for UInt64Type {
    fn write(value: u64) -> Vec<u8> {
        value.to_be_bytes().to_vec()
    }
}

impl PrimitiveWriter<u8> for UInt8Type {
    fn write(value: u8) -> Vec<u8> {
        value.to_be_bytes().to_vec()
    }
}

impl PrimitiveWriter<bool> for BooleanType {
    fn write(value: bool) -> Vec<u8> {
        if value {
            vec![1]
        } else {
            vec![0]
        }
    }
}

impl PrimitiveWriter<String> for Utf8Type {
    fn write(value: String) -> Vec<u8> {
        value.as_bytes().to_vec()
    }
}

impl PrimitiveWriter<Vec<String>> for ListType {
    fn write(value: Vec<String>) -> Vec<u8> {
        let mut buffer = Vec::new();
        for v in value {
            let len = v.len() as f32;
            buffer.extend_from_slice(&len.to_be_bytes());
            buffer.extend_from_slice(v.as_bytes());
        }
        buffer
    }
}
