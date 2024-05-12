use tfhe::{prelude::FheTrivialEncrypt, FheBool, FheUint8};

use crate::bitops::*;

////////////////////////////////////////////////////////////////////////////////
// ThreadSafeBool
////////////////////////////////////////////////////////////////////////////////

pub trait ThreadSafeBool:
    Send
    + Sync
    + RefNot
    + RefBitAnd<Output = Self>
    + RefBitOr<Output = Self>
    + BooleanType
    + DebugToString
    + Clone
{
}

impl<T> ThreadSafeBool for T where
    T: Send
        + Sync
        + RefNot
        + RefBitAnd<Output = T>
        + RefBitOr<Output = T>
        + BooleanType
        + DebugToString
        + Clone
{
}

////////////////////////////////////////////////////////////////////////////////
// ThreadSafeUInt
////////////////////////////////////////////////////////////////////////////////

pub trait ThreadSafeUInt:
    Send
    + Sync
    + RefNot
    + RefBitAnd<Output = Self>
    + RefBitOr<Output = Self>
    + UIntType
    + DebugToString
    + Clone
{
}

impl<T> ThreadSafeUInt for T where
    T: Send
        + Sync
        + RefNot
        + RefBitAnd<Output = T>
        + RefBitOr<Output = T>
        + UIntType
        + DebugToString
        + Clone
{
}

////////////////////////////////////////////////////////////////////////////////
// BooleanType
////////////////////////////////////////////////////////////////////////////////

pub trait BooleanType {
    fn get_true() -> Self;
    fn get_false() -> Self;
}

impl<T, const N: usize> BooleanType for [T; N]
where
    T: BooleanType,
{
    #[inline(always)]
    fn get_true() -> Self {
        let a: [T; N] = std::array::from_fn(|_| T::get_true());
        a
    }
    #[inline(always)]
    fn get_false() -> Self {
        let a: [T; N] = std::array::from_fn(|_| T::get_false());
        a
    }
}

impl BooleanType for bool {
    #[inline(always)]
    fn get_true() -> Self {
        true
    }
    #[inline(always)]
    fn get_false() -> Self {
        false
    }
}

impl BooleanType for FheBool {
    #[inline(always)]
    fn get_true() -> Self {
        <FheBool as tfhe::prelude::FheTrivialEncrypt<bool>>::encrypt_trivial(true)
    }
    #[inline(always)]
    fn get_false() -> Self {
        <FheBool as tfhe::prelude::FheTrivialEncrypt<bool>>::encrypt_trivial(false)
    }
}

////////////////////////////////////////////////////////////////////////////////
// UIntType
////////////////////////////////////////////////////////////////////////////////

pub trait UIntType {
    fn get_zero() -> Self;
    fn get_max() -> Self;
}

impl<T, const N: usize> UIntType for [T; N]
where
    T: UIntType,
{
    #[inline(always)]
    fn get_zero() -> Self {
        let a: [T; N] = std::array::from_fn(|_| T::get_zero());
        a
    }
    #[inline(always)]
    fn get_max() -> Self {
        let a: [T; N] = std::array::from_fn(|_| T::get_max());
        a
    }
}

impl UIntType for FheUint8 {
    #[inline(always)]
    fn get_zero() -> Self {
        FheUint8::encrypt_trivial(0_u8)
    }
    #[inline(always)]
    fn get_max() -> Self {
        FheUint8::encrypt_trivial(u8::MAX)
    }
}

impl UIntType for u8 {
    #[inline(always)]
    fn get_zero() -> Self {
        0_u8
    }
    #[inline(always)]
    fn get_max() -> Self {
        u8::MAX
    }
}

impl UIntType for FheBool {
    #[inline(always)]
    fn get_zero() -> Self {
        FheBool::encrypt_trivial(false)
    }
    #[inline(always)]
    fn get_max() -> Self {
        FheBool::encrypt_trivial(true)
    }
}

impl UIntType for bool {
    #[inline(always)]
    fn get_zero() -> Self {
        false
    }
    #[inline(always)]
    fn get_max() -> Self {
        true
    }
}

////////////////////////////////////////////////////////////////////////////////
// MemoryCastInto
////////////////////////////////////////////////////////////////////////////////

pub trait MemoryCastInto<T> {
    fn mem_cast_into(self) -> T;
}

////////////////////////////////////////////////////////////////////////////////
// DebugToString
////////////////////////////////////////////////////////////////////////////////

pub trait DebugToString {
    fn debug_to_string(&self) -> String {
        "".to_string()
    }
}

impl<T, const N: usize> DebugToString for [T; N]
where
    T: DebugToString,
{
    fn debug_to_string(&self) -> String {
        let mut s = vec![];
        self.iter().for_each(|x| {
            s.push(x.debug_to_string());
        });
        s.join(", ")
    }
}

impl DebugToString for bool {
    fn debug_to_string(&self) -> String {
        self.to_string()
    }
}

impl DebugToString for u8 {
    fn debug_to_string(&self) -> String {
        format!("{:#04x}", self)
    }
}

impl DebugToString for FheBool {
    fn debug_to_string(&self) -> String {
        "[FheBool=??]".to_string()
    }
}

impl DebugToString for FheUint8 {
    fn debug_to_string(&self) -> String {
        "[FheUint8=??]".to_string()
    }
}

