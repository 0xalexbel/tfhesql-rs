use std::marker::PhantomData;
use tfhe::prelude::{FheEncrypt, FheTryEncrypt, IfThenElse};
use tfhe::CompactFheBool;
use tfhe::CompactFheUint8;
use tfhe::CompressedFheBool;
use tfhe::CompressedFheUint8;
use tfhe::{ClientKey, CompactPublicKey, FheBool, FheUint8};

pub trait ValueFrom<T> {
    fn value_from(value: T) -> Self;
}

impl<T, U, const N: usize> ValueFrom<[T; N]> for [U; N]
where
    for<'a> U: ValueFrom<&'a T>,
{
    fn value_from(value: [T; N]) -> Self {
        let a: [U; N] = std::array::from_fn(|i| U::value_from(&value[i]));
        a
    }
}

impl<T, U, const N: usize> ValueFrom<&[T; N]> for [U; N]
where
    for<'a> U: ValueFrom<&'a T>,
{
    fn value_from(value: &[T; N]) -> Self {
        let a: [U; N] = std::array::from_fn(|i| U::value_from(&value[i]));
        a
    }
}

impl<T, U> ValueFrom<&Vec<T>> for Vec<U>
where
    for<'a> U: ValueFrom<&'a T>,
{
    fn value_from(value: &Vec<T>) -> Self {
        value.iter().map(|x| U::value_from(x)).collect()
    }
}

impl ValueFrom<u8> for u8 {
    #[inline(always)]
    fn value_from(value: u8) -> u8 {
        value
    }
}

impl ValueFrom<&u8> for u8 {
    #[inline(always)]
    fn value_from(value: &u8) -> u8 {
        *value
    }
}

impl ValueFrom<bool> for bool {
    #[inline(always)]
    fn value_from(value: bool) -> bool {
        value
    }
}

impl ValueFrom<FheBool> for FheBool {
    #[inline(always)]
    fn value_from(value: FheBool) -> FheBool {
        value
    }
}

impl ValueFrom<&bool> for bool {
    #[inline(always)]
    fn value_from(value: &bool) -> bool {
        *value
    }
}

impl ValueFrom<u8> for FheUint8 {
    #[inline(always)]
    fn value_from(value: u8) -> FheUint8 {
        <FheUint8 as tfhe::prelude::FheTrivialEncrypt<u8>>::encrypt_trivial(value)
    }
}

impl ValueFrom<&u8> for FheUint8 {
    #[inline(always)]
    fn value_from(value: &u8) -> FheUint8 {
        <FheUint8 as tfhe::prelude::FheTrivialEncrypt<u8>>::encrypt_trivial(*value)
    }
}

impl ValueFrom<FheUint8> for FheUint8 {
    #[inline(always)]
    fn value_from(value: FheUint8) -> FheUint8 {
        value
    }
}

impl ValueFrom<&FheBool> for FheUint8 {
    #[inline(always)]
    fn value_from(value: &FheBool) -> FheUint8 {
        #[cfg(feature = "stats")]
        crate::stats::inc_u8_if_then_else();
        value.if_then_else(
            &<FheUint8 as tfhe::prelude::FheTrivialEncrypt<u8>>::encrypt_trivial(u8::MAX),
            &<FheUint8 as tfhe::prelude::FheTrivialEncrypt<u8>>::encrypt_trivial(0_u8),
        )
    }
}

impl ValueFrom<bool> for u8 {
    #[inline(always)]
    fn value_from(value: bool) -> u8 {
        if value {
            u8::MAX
        } else {
            0_u8
        }
    }
}

impl ValueFrom<&bool> for u8 {
    #[inline(always)]
    fn value_from(value: &bool) -> u8 {
        if *value {
            u8::MAX
        } else {
            0_u8
        }
    }
}

/// Into::<T>(Self::Default())
pub trait DefaultInto<T> {
    fn default_into() -> T;
}

/// IntoWithKey::<T>(Self::Default(), key)
pub trait DefaultIntoWithKey<T, Key> {
    fn default_into_with_key(key: &Key) -> T;
}

// Convert bool into FheBool
// let a: FheBool = bool::default_into()
impl DefaultInto<FheBool> for bool {
    #[inline(always)]
    fn default_into() -> FheBool {
        <FheBool as tfhe::prelude::FheTrivialEncrypt<bool>>::encrypt_trivial(false)
    }
}

impl DefaultInto<FheUint8> for u8 {
    #[inline(always)]
    fn default_into() -> FheUint8 {
        <FheUint8 as tfhe::prelude::FheTrivialEncrypt<u8>>::encrypt_trivial(0_u8)
    }
}

impl DefaultInto<bool> for FheBool {
    #[inline(always)]
    fn default_into() -> bool {
        false
    }
}

impl DefaultInto<u8> for FheUint8 {
    #[inline(always)]
    fn default_into() -> u8 {
        0_u8
    }
}

impl DefaultInto<bool> for bool {
    #[inline(always)]
    fn default_into() -> bool {
        false
    }
}

impl DefaultInto<u8> for u8 {
    #[inline(always)]
    fn default_into() -> u8 {
        0_u8
    }
}

impl DefaultInto<usize> for usize {
    #[inline(always)]
    fn default_into() -> usize {
        0
    }
}

impl DefaultInto<FheBool> for FheBool {
    #[inline(always)]
    fn default_into() -> FheBool {
        <FheBool as tfhe::prelude::FheTrivialEncrypt<bool>>::encrypt_trivial(false)
    }
}

impl DefaultInto<FheUint8> for FheUint8 {
    #[inline(always)]
    fn default_into() -> FheUint8 {
        <FheUint8 as tfhe::prelude::FheTrivialEncrypt<u8>>::encrypt_trivial(0_u8)
    }
}

impl DefaultInto<FheBool> for CompressedFheBool {
    #[inline(always)]
    fn default_into() -> FheBool {
        <FheBool as tfhe::prelude::FheTrivialEncrypt<bool>>::encrypt_trivial(false)
    }
}

impl DefaultInto<FheUint8> for CompressedFheUint8 {
    #[inline(always)]
    fn default_into() -> FheUint8 {
        <FheUint8 as tfhe::prelude::FheTrivialEncrypt<u8>>::encrypt_trivial(0_u8)
    }
}

impl DefaultInto<bool> for CompressedFheBool {
    #[inline(always)]
    fn default_into() -> bool {
        false
    }
}

impl DefaultInto<u8> for CompressedFheUint8 {
    #[inline(always)]
    fn default_into() -> u8 {
        0_u8
    }
}

impl DefaultInto<FheBool> for CompactFheBool {
    #[inline(always)]
    fn default_into() -> FheBool {
        <FheBool as tfhe::prelude::FheTrivialEncrypt<bool>>::encrypt_trivial(false)
    }
}

impl DefaultInto<FheUint8> for CompactFheUint8 {
    #[inline(always)]
    fn default_into() -> FheUint8 {
        <FheUint8 as tfhe::prelude::FheTrivialEncrypt<u8>>::encrypt_trivial(0_u8)
    }
}

impl DefaultInto<bool> for CompactFheBool {
    #[inline(always)]
    fn default_into() -> bool {
        false
    }
}

impl DefaultInto<u8> for CompactFheUint8 {
    #[inline(always)]
    fn default_into() -> u8 {
        0_u8
    }
}

impl<C, E> DefaultInto<PhantomData<E>> for PhantomData<C> {
    #[inline(always)]
    fn default_into() -> PhantomData<E> {
        Default::default()
    }
}

impl<C, E> DefaultInto<Vec<E>> for Vec<C>
where
    C: DefaultInto<E>,
{
    #[inline(always)]
    fn default_into() -> Vec<E> {
        Vec::<E>::new()
    }
}

impl<Key> DefaultIntoWithKey<FheBool, Key> for bool
where
    FheBool: FheTryEncrypt<bool, Key>,
{
    #[inline(always)]
    fn default_into_with_key(key: &Key) -> FheBool {
        <FheBool as tfhe::prelude::FheEncrypt<bool, Key>>::encrypt(false, key)
        //<FheBool as tfhe::prelude::FheTrivialEncrypt<bool>>::encrypt_trivial(false)
    }
}

impl<Key> DefaultIntoWithKey<FheUint8, Key> for u8
where
    FheUint8: FheTryEncrypt<u8, Key>,
{
    #[inline(always)]
    fn default_into_with_key(key: &Key) -> FheUint8 {
        <FheUint8 as tfhe::prelude::FheEncrypt<u8, Key>>::encrypt(0, key)
        //<FheUint8 as tfhe::prelude::FheTrivialEncrypt<u8>>::encrypt_trivial(0_u8)
    }
}

impl<Key> DefaultIntoWithKey<bool, Key> for bool {
    #[inline(always)]
    fn default_into_with_key(_: &Key) -> bool {
        false
    }
}

impl<Key> DefaultIntoWithKey<u8, Key> for u8 {
    #[inline(always)]
    fn default_into_with_key(_: &Key) -> u8 {
        0_u8
    }
}

impl<Key> DefaultIntoWithKey<usize, Key> for usize {
    #[inline(always)]
    fn default_into_with_key(_: &Key) -> usize {
        0
    }
}

impl<Key> DefaultIntoWithKey<FheBool, Key> for FheBool
where
    FheBool: FheTryEncrypt<bool, Key>,
{
    #[inline(always)]
    fn default_into_with_key(key: &Key) -> FheBool {
        <FheBool as FheEncrypt<bool, Key>>::encrypt(false, key)
        //<FheBool as tfhe::prelude::FheTrivialEncrypt<bool>>::encrypt_trivial(false)
    }
}

impl<Key> DefaultIntoWithKey<FheUint8, Key> for FheUint8
where
    FheUint8: FheTryEncrypt<u8, Key>,
{
    #[inline(always)]
    fn default_into_with_key(key: &Key) -> FheUint8 {
        <FheUint8 as FheEncrypt<u8, Key>>::encrypt(0, key)
        //<FheUint8 as tfhe::prelude::FheTrivialEncrypt<u8>>::encrypt_trivial(0_u8)
    }
}

impl DefaultIntoWithKey<CompressedFheBool, ClientKey> for bool {
    #[inline(always)]
    fn default_into_with_key(key: &ClientKey) -> CompressedFheBool {
        <CompressedFheBool as tfhe::prelude::FheEncrypt<bool, ClientKey>>::encrypt(false, key)
    }
}

impl DefaultIntoWithKey<CompressedFheUint8, ClientKey> for u8 {
    #[inline(always)]
    fn default_into_with_key(key: &ClientKey) -> CompressedFheUint8 {
        <CompressedFheUint8 as tfhe::prelude::FheEncrypt<u8, ClientKey>>::encrypt(0_u8, key)
    }
}

impl DefaultIntoWithKey<CompressedFheBool, ClientKey> for CompressedFheBool {
    #[inline(always)]
    fn default_into_with_key(key: &ClientKey) -> CompressedFheBool {
        <CompressedFheBool as tfhe::prelude::FheEncrypt<bool, ClientKey>>::encrypt(false, key)
    }
}

impl DefaultIntoWithKey<CompressedFheUint8, ClientKey> for CompressedFheUint8 {
    #[inline(always)]
    fn default_into_with_key(key: &ClientKey) -> CompressedFheUint8 {
        <CompressedFheUint8 as tfhe::prelude::FheEncrypt<u8, ClientKey>>::encrypt(0_u8, key)
    }
}

impl DefaultIntoWithKey<CompactFheBool, CompactPublicKey> for bool {
    #[inline(always)]
    fn default_into_with_key(key: &CompactPublicKey) -> CompactFheBool {
        <CompactFheBool as tfhe::prelude::FheEncrypt<bool, CompactPublicKey>>::encrypt(false, key)
    }
}

impl DefaultIntoWithKey<CompactFheUint8, CompactPublicKey> for u8 {
    #[inline(always)]
    fn default_into_with_key(key: &CompactPublicKey) -> CompactFheUint8 {
        <CompactFheUint8 as tfhe::prelude::FheEncrypt<u8, CompactPublicKey>>::encrypt(0_u8, key)
    }
}

impl<C, E, Key> DefaultIntoWithKey<PhantomData<E>, Key> for PhantomData<C> {
    #[inline(always)]
    fn default_into_with_key(_: &Key) -> PhantomData<E> {
        Default::default()
    }
}

impl<C, E, Key> DefaultIntoWithKey<Vec<E>, Key> for Vec<C>
where
    C: DefaultIntoWithKey<E, Key>,
{
    #[inline(always)]
    fn default_into_with_key(_: &Key) -> Vec<E> {
        vec![]
    }
}

impl<C, E> DefaultInto<(E,)> for (C,)
where
    C: DefaultInto<E>,
{
    #[inline]
    fn default_into() -> (E,) {
        (C::default_into(),)
    }
}

impl<C, E, Key> DefaultIntoWithKey<(E,), Key> for (C,)
where
    C: DefaultIntoWithKey<E, Key>,
{
    #[inline]
    fn default_into_with_key(key: &Key) -> (E,) {
        (C::default_into_with_key(key),)
    }
}

impl<C, E, const N: usize> DefaultInto<Box<[E; N]>> for Box<[C; N]>
where
    C: DefaultInto<E>,
{
    #[inline]
    fn default_into() -> Box<[E; N]> {
        let mut v: Vec<E> = Vec::with_capacity(N);
        v.extend(std::iter::repeat(N).map(|_| C::default_into()));
        assert_eq!(v.len(), N);
        let boxed_slice = v.into_boxed_slice();
        let boxed_array: Box<[E; N]> = match boxed_slice.try_into() {
            Ok(ba) => ba,
            Err(o) => panic!("Expected a Vec of length {} but it was {}", N, o.len()),
        };
        boxed_array
    }
}

impl<C, E, Key, const N: usize> DefaultIntoWithKey<Box<[E; N]>, Key> for Box<[C; N]>
where
    C: DefaultIntoWithKey<E, Key>,
{
    #[inline]
    fn default_into_with_key(key: &Key) -> Box<[E; N]> {
        let mut v: Vec<E> = Vec::with_capacity(N);
        v.extend(std::iter::repeat(N).map(|_| C::default_into_with_key(key)));
        assert_eq!(v.len(), N);
        let boxed_slice = v.into_boxed_slice();
        let boxed_array: Box<[E; N]> = match boxed_slice.try_into() {
            Ok(ba) => ba,
            Err(o) => panic!("Expected a Vec of length {} but it was {}", N, o.len()),
        };
        boxed_array
    }
}

impl<C, E, const N: usize> DefaultInto<[E; N]> for [C; N]
where
    C: DefaultInto<E>,
{
    #[inline]
    fn default_into() -> [E; N] {
        let a: [E; N] = std::array::from_fn(|_| C::default_into());
        a
    }
}

impl<C, E, Key, const N: usize> DefaultIntoWithKey<[E; N], Key> for [C; N]
where
    C: DefaultIntoWithKey<E, Key>,
{
    #[inline]
    fn default_into_with_key(key: &Key) -> [E; N] {
        let a: [E; N] = std::array::from_fn(|_| C::default_into_with_key(key));
        a
    }
}

#[allow(unused_macros)]
macro_rules! drv_wrapper_default_into {
    (
        $TheStruct:ident<$CParam:ident>($type1:ty)
    ) => {
        impl<$CParam, E> DefaultInto<$TheStruct<E>> for $TheStruct<$CParam>
        where
            $CParam: DefaultInto<E>,
        {
            #[inline]
            fn default_into() -> $TheStruct<E> {
                $TheStruct::<E>(<$type1>::default_into())
            }
        }
    };
}
#[allow(unused_imports)]
pub(crate) use drv_wrapper_default_into;

#[allow(unused_macros)]
macro_rules! drv_default_into {
    (
        $TheStruct:ident<$CParam:ident>
        {$($field1:ident:$type1:ty,)+}
    ) => {
        impl<$CParam, E> DefaultInto<$TheStruct<E>> for $TheStruct<$CParam>
        where
            $CParam: DefaultInto<E>,
        {
            #[inline]
            fn default_into() -> $TheStruct<E> {
                $TheStruct::<E> {
                    $(
                        $field1: <$type1>::default_into(),
                    )*
                }
            }
        }
    };
}
#[allow(unused_imports)]
pub(crate) use drv_default_into;

#[allow(unused_macros)]
macro_rules! drv_default_into2 {
    (
        $TheStruct:ident<$CParam1:ident, $CParam2:ident>
        {$($field:ident:$type:ty,)+}
    ) => {
        impl<$CParam1, $CParam2, E1, E2> DefaultInto<$TheStruct<E1, E2>> for $TheStruct<$CParam1, $CParam2>
        where
            $CParam1: DefaultInto<E1>,
            $CParam2: DefaultInto<E2>,
        {
            #[inline]
            fn default_into() -> $TheStruct<E1, E2> {
                $TheStruct::<E1, E2> {
                    $(
                        $field: <$type>::default_into(),
                    )*
                }
            }
        }
    };
}
#[allow(unused_imports)]
pub(crate) use drv_default_into2;

#[allow(unused_macros)]
macro_rules! drv_wrapper_default_into_with_key {
    (
        $TheStruct:ident<$CParam:ident>($type1:ty)
    ) => {
        impl<$CParam, E, Key> DefaultIntoWithKey<$TheStruct<E>, Key> for $TheStruct<$CParam>
        where
            $CParam: DefaultIntoWithKey<E, Key>,
        {
            #[inline]
            fn default_into_with_key(key: &Key) -> $TheStruct<E> {
                $TheStruct::<E>(<$type1>::default_into_with_key(key))
            }
        }
    };
}
#[allow(unused_imports)]
pub(crate) use drv_wrapper_default_into_with_key;

#[allow(unused_macros)]
macro_rules! drv_default_into_with_key {
    (
        $TheStruct:ident<$CParam:ident>
        {$($field1:ident:$type1:ty,)+}
    ) => {
        impl<$CParam, E, Key> DefaultIntoWithKey<$TheStruct<E>, Key> for $TheStruct<$CParam>
        where
            $CParam: DefaultIntoWithKey<E, Key>,
        {
            #[inline]
            fn default_into_with_key(key: &Key) -> $TheStruct<E> {
                $TheStruct::<E> {
                    $(
                        $field1: <$type1>::default_into_with_key(key),
                    )*
                }
            }
        }
    };
}
#[allow(unused_imports)]
pub(crate) use drv_default_into_with_key;

#[allow(unused_macros)]
macro_rules! drv_default_into_with_key2 {
    (
        $TheStruct:ident<$CParam1:ident, $CParam2:ident>
        {$($field:ident:$type:ty,)+}
    ) => {
        impl<$CParam1, $CParam2, E1, E2, Key> DefaultIntoWithKey<$TheStruct<E1, E2>, Key> for $TheStruct<$CParam1, $CParam2>
        where
            $CParam1: DefaultIntoWithKey<E1, Key>,
            $CParam2: DefaultIntoWithKey<E2, Key>,
        {
            #[inline]
            fn default_into_with_key(key: &Key) -> $TheStruct<E1, E2> {
                $TheStruct::<E1, E2> {
                    $(
                        $field: <$type>::default_into_with_key(key),
                    )*
                }
            }
        }
    };
}
#[allow(unused_imports)]
pub(crate) use drv_default_into_with_key2;

////////////////////////////////////////////////////////////////////////////////
// Tests
////////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod test {
    use super::*;
    use crate::test_util::try_load_or_gen_test_keys;
    use crate::test_util::broadcast_set_server_key;
    use tfhe::{prelude::FheDecrypt, set_server_key};

    struct AStruct<T> {
        a: BStruct<T>,
        b: T,
        c: Vec<T>,
        d: Vec<BStruct<T>>,
    }

    struct BStruct<T> {
        e: T,
        f: T,
    }

    drv_default_into! { BStruct<T> {e:T, f:T,} }
    drv_default_into! { AStruct<T> {a:BStruct<T>, b:T, c:Vec<T>, d:Vec<BStruct<T>>,} }

    #[test]
    fn test_default_into() {
        let (ck, sk) = try_load_or_gen_test_keys(false);

        broadcast_set_server_key(&sk);
        set_server_key(sk);

        let enc_b: FheBool = bool::default_into();
        let clear_b = enc_b.decrypt(&ck);
        assert!(!clear_b);

        let enc_b: FheBool = FheBool::default_into();
        let clear_b = enc_b.decrypt(&ck);
        assert!(!clear_b);

        let enc_b: FheBool = bool::default_into();
        let clear_b = enc_b.try_decrypt_trivial().unwrap();
        assert!(!clear_b);

        let enc_b: FheBool = FheBool::default_into();
        let clear_b = enc_b.try_decrypt_trivial().unwrap();
        assert!(!clear_b);

        let clear_b: bool = bool::default_into();
        assert!(!clear_b);

        let clear_b: bool = FheBool::default_into();
        assert!(!clear_b);

        let enc_v: Vec<Vec<bool>> = Vec::<Vec<FheBool>>::default_into();
        assert_eq!(enc_v.len(), 0);

        let enc_v: Vec<Vec<FheBool>> = Vec::<Vec<FheBool>>::default_into();
        assert_eq!(enc_v.len(), 0);

        let enc_s: BStruct<FheBool> = BStruct::<bool>::default_into();
        assert!(!enc_s.e.decrypt(&ck));
        assert!(!enc_s.f.decrypt(&ck));

        let enc_s: BStruct<bool> = BStruct::<bool>::default_into();
        assert!(!enc_s.e);
        assert!(!enc_s.f);

        let enc_s: AStruct<FheBool> = AStruct::<bool>::default_into();
        assert!(!enc_s.a.e.decrypt(&ck));
        assert!(!enc_s.a.f.decrypt(&ck));
        assert!(!enc_s.b.decrypt(&ck));
        assert_eq!(enc_s.c.len(), 0);
        assert_eq!(enc_s.d.len(), 0);

        let enc_s: AStruct<bool> = AStruct::<bool>::default_into();
        assert!(!enc_s.a.e);
        assert!(!enc_s.a.f);
        assert!(!enc_s.b);
        assert_eq!(enc_s.c.len(), 0);
        assert_eq!(enc_s.d.len(), 0);
    }
}
