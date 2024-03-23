use arrow_format::ipc::Bool;

use crate::io::datatypes::*;

// pub enum BinTableTypes {
//     L(Vec<BooleanType>), // Logical
//     X(Vec<BooleanType>), // Bit
//     B(Vec<Int8Type>), // Byte
//     I(Vec<Int16Type>), // Short
//     J(Vec<Int32Type>), // Int
//     K(Vec<Int64Type>), // Long
//     A(Vec<Utf8Type>), // Char
//     E(Vec<Float32Type>), // Float
//     D(Vec<Float64Type>), // Double
//     C(Vec<String>), // Complex
//     M(Vec<String>), // Double complex
//     P(Vec<String>), // Array descriptor
//     Q(Vec<String>), // Array descriptor
// }

pub enum BinTableTypes {
    LLogical, // Logical
    XBit, // Bit
    BByte, // Byte
    IShort, // Short
    JInt, // Int
    KLong, // Long
    AAhar, // Char
    Efloat, // Float
    DDouble, // Double
    CComplex, // Complex
    //TODO: Implement the following types
    MComplex, // Double complex
    PArray, // Array descriptor
    QArray, // Array descriptor
}

trait get_type_letter {
    fn get_type_letter(&self) -> char;
}

impl get_type_letter for BooleanType {
    fn get_type_letter(&self) -> char {
        'L'
    }
}