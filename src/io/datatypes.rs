pub(crate) trait PrimitiveParser<T> {
    fn parse(bytes: &[u8]) -> Option<T>;
}

pub struct Float32Type;
pub struct Float64Type;
pub struct Int16Type;
pub struct Int32Type;
pub struct Int64Type;
pub struct Int8Type;
pub struct UInt16Type;
pub struct UInt32Type;
pub struct UInt64Type;
pub struct UInt8Type;
pub struct BooleanType;
pub struct Utf8Type;
pub struct ListType;

impl PrimitiveParser<f32> for Float32Type {
    fn parse(bytes: &[u8]) -> Option<f32> {
        
    }
}

impl PrimitiveParser<f64> for Float64Type {
    fn parse(bytes: &[u8]) -> Option<f64> {
        unimplemented!()
    }
}

impl PrimitiveParser<i16> for Int16Type {
    fn parse(bytes: &[u8]) -> Option<i16> {
        unimplemented!()
    }
}

impl PrimitiveParser<i32> for Int32Type {
    fn parse(bytes: &[u8]) -> Option<i32> {
        unimplemented!()
    }
}

impl PrimitiveParser<i64> for Int64Type {
    fn parse(bytes: &[u8]) -> Option<i64> {
        unimplemented!()
    }
}

impl PrimitiveParser<i8> for Int8Type {
    fn parse(bytes: &[u8]) -> Option<i8> {
        unimplemented!()
    }
}

impl PrimitiveParser<u16> for UInt16Type {
    fn parse(bytes: &[u8]) -> Option<u16> {
        unimplemented!()
    }
}

impl PrimitiveParser<u32> for UInt32Type {
    fn parse(bytes: &[u8]) -> Option<u32> {
        unimplemented!()
    }
}

impl PrimitiveParser<u64> for UInt64Type {
    fn parse(bytes: &[u8]) -> Option<u64> {
        unimplemented!()
    }
}

impl PrimitiveParser<u8> for UInt8Type {
    fn parse(bytes: &[u8]) -> Option<u8> {
        unimplemented!()
    }
}

impl PrimitiveParser<bool> for BooleanType {
    fn parse(bytes: &[u8]) -> Option<bool> {
        unimplemented!()
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
        unimplemented!()
    }
}
