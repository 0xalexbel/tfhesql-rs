pub mod traits;

use tfhe::prelude::{FheTrivialEncrypt, FheTryEncrypt, FheEncrypt};
use tfhe::{
    ClientKey, CompactFheBool, CompactFheUint8, CompactPublicKey, CompressedFheBool, CompressedFheUint8, FheBool, FheUint8
};
use rayon::iter::IndexedParallelIterator;
use rayon::iter::IntoParallelRefIterator;
use rayon::iter::IntoParallelRefMutIterator;
use rayon::iter::ParallelIterator;
use crate::default_into::{DefaultInto, DefaultIntoWithKey};
use self::traits::*;

////////////////////////////////////////////////////////////////////////////////
// ENCRYPT
////////////////////////////////////////////////////////////////////////////////

// Must redefine our own traits to add encrypt/decrypt support on Vec
impl<Key> EncryptRef<bool, Key> for FheBool
where
    FheBool: FheTryEncrypt<bool, Key>,
{
    #[inline]
    fn encrypt_ref(value: &bool, key: &Key) -> Self {
        <FheBool as FheEncrypt<bool, Key>>::encrypt(*value, key)
    }
}

impl EncryptRef<bool, ClientKey> for CompressedFheBool {
    #[inline]
    fn encrypt_ref(value: &bool, key: &ClientKey) -> Self {
        <CompressedFheBool as FheEncrypt<bool, ClientKey>>::encrypt(*value, key)
    }
}

impl EncryptRef<bool, CompactPublicKey> for CompactFheBool {
    #[inline]
    fn encrypt_ref(value: &bool, key: &CompactPublicKey) -> Self {
        <CompactFheBool as FheEncrypt<bool, CompactPublicKey>>::encrypt(*value, key)
    }
}

impl<Key> EncryptRef<u8, Key> for FheUint8
where
    FheUint8: FheTryEncrypt<u8, Key>,
{
    #[inline]
    fn encrypt_ref(value: &u8, key: &Key) -> Self {
        <FheUint8 as FheEncrypt<u8, Key>>::encrypt(*value, key)
    }
}

impl EncryptRef<u8, ClientKey> for CompressedFheUint8 {
    #[inline]
    fn encrypt_ref(value: &u8, key: &ClientKey) -> Self {
        <CompressedFheUint8 as FheEncrypt<u8, ClientKey>>::encrypt(*value, key)
    }
}

impl EncryptRef<u8, CompactPublicKey> for CompactFheUint8 {
    #[inline]
    fn encrypt_ref(value: &u8, key: &CompactPublicKey) -> Self {
        <CompactFheUint8 as FheEncrypt<u8, CompactPublicKey>>::encrypt(*value, key)
    }
}

impl<E, C, Key> EncryptRef<Vec<C>, Key> for Vec<E>
where
    E: EncryptRef<C, Key> + Clone + Send,
    C: DefaultIntoWithKey<E, Key> + Sync,
    Key: Sync
{
    fn encrypt_ref(value: &Vec<C>, key: &Key) -> Self {
        let def = C::default_into_with_key(key);
        let mut result = vec![def; value.len()];
        result
            .par_iter_mut()
            .zip(value.par_iter())
            .for_each(|(dst, v)| *dst = E::encrypt_ref(v, key));
        result
    }
}

impl<E, C, Key> EncryptRef<(C,), Key> for (E,)
where
    E: EncryptRef<C, Key>
{
    fn encrypt_ref(value: &(C,), key: &Key) -> Self {
        (E::encrypt_ref(&value.0, key),)
    }
}

impl<E, C, Key, const N: usize> EncryptRef<Box<[C; N]>, Key> for Box<[E; N]>
where
    E: EncryptRef<C, Key> + Send,
    C: DefaultIntoWithKey<E, Key> + Sync,
    Key: Sync,
{
    fn encrypt_ref(value: &Box<[C; N]>, key: &Key) -> Self {
        let mut box_result = Box::<[C; N]>::default_into_with_key(key);
        box_result
            .par_iter_mut()
            .zip(value.par_iter())
            .for_each(|(dst, v)| *dst = E::encrypt_ref(v, key));
        box_result
    }
}

////////////////////////////////////////////////////////////////////////////////
// ENCRYPT MACROS
////////////////////////////////////////////////////////////////////////////////

#[allow(unused_macros)]
macro_rules! drv_wrapper_encrypt {
    ( 
        $TheStruct:ident<$EParam:ident>
    ) => {
        impl<$EParam, C, Key> EncryptRef<$TheStruct<C>, Key> for $TheStruct<$EParam>
        where
            C: DefaultIntoWithKey<$EParam, Key> + Sync,
            $EParam: EncryptRef<C, Key> + Clone + Send,
            Key: Sync
        {
            #[inline]
            fn encrypt_ref(value: &$TheStruct<C>, key: &Key) -> Self {
                $TheStruct::<$EParam> (
                    value.0.encrypt_into(key)
                )
            }
        }
    };
}
#[allow(unused_imports)]
pub(crate) use drv_wrapper_encrypt;

#[allow(unused_macros)]
macro_rules! drv1_encrypt {
    ( 
        $TheStruct:ident<$EParam:ident>
        {$field1:ident:$type1:ty}
    ) => {
        impl<$EParam, C, Key> EncryptRef<$TheStruct<C>, Key> for $TheStruct<$EParam>
        where
            C: DefaultIntoWithKey<$EParam, Key> + Sync,
            $EParam: EncryptRef<C, Key> + Clone + Send,
            Key: Sync
        {
            #[inline]
            fn encrypt_ref(value: &$TheStruct<C>, key: &Key) -> Self {
                $TheStruct::<$EParam> {
                    $field1: value.$field1.encrypt_into(key),
                }
            }
        }
    };
}
#[allow(unused_imports)]
pub(crate) use drv1_encrypt;

#[allow(unused_macros)]
macro_rules! drv1_encrypt2 {
    ( 
        $TheStruct:ident<$EParam1:ident, $EParam2:ident>
        {$field1:ident:$type1:ty}
    ) => {
        impl<$EParam1, $EParam2, C1, C2, Key> EncryptRef<$TheStruct<C1, C2>, Key> for $TheStruct<$EParam1, $EParam2>
        where
            C1: DefaultIntoWithKey<$EParam1, Key> + Sync,
            C2: DefaultIntoWithKey<$EParam2, Key> + Sync,
            $EParam1: EncryptRef<C1, Key> + Clone + Send,
            $EParam2: EncryptRef<C2, Key> + Clone + Send,
            Key: Sync
        {
            #[inline]
            fn encrypt_ref(value: &$TheStruct<C1, C2>, key: &Key) -> Self {
                $TheStruct::<$EParam1, $EParam2> {
                    $field1: value.$field1.encrypt_into(key),
                }
            }
        }
    };
}
#[allow(unused_imports)]
pub(crate) use drv1_encrypt2;

#[allow(unused_macros)]
macro_rules! drv2_encrypt {
    ( 
        $TheStruct:ident<$EParam:ident>
        {$field1:ident:$type1:ty, $field2:ident:$type2:ty}
    ) => {
        impl<$EParam, C, Key> EncryptRef<$TheStruct<C>, Key> for $TheStruct<$EParam>
        where
            C: DefaultIntoWithKey<$EParam, Key> + Sync,
            $EParam: EncryptRef<C, Key> + Clone + Send,
            Key: Sync
        {
            #[inline]
            fn encrypt_ref(value: &$TheStruct<C>, key: &Key) -> Self {
                let c1 = &value.$field1;
                let c2 = &value.$field2;
                let (e1, e2) = rayon::join(|| c1.encrypt_into(key), || c2.encrypt_into(key));
                $TheStruct::<$EParam> {
                    $field1: e1,
                    $field2: e2,
                }
            }
        }
    };
}
#[allow(unused_imports)]
pub(crate) use drv2_encrypt;

#[allow(unused_macros)]
macro_rules! drv2_encrypt2 {
    ( 
        $TheStruct:ident<$EParam1:ident, $EParam2:ident>
        {$field1:ident:$type1:ty, $field2:ident:$type2:ty}
    ) => {
        impl<$EParam1, $EParam2, C1, C2, Key> EncryptRef<$TheStruct<C1, C2>, Key> for $TheStruct<$EParam1, $EParam2>
        where
            C1: DefaultIntoWithKey<$EParam1, Key> + Sync,
            C2: DefaultIntoWithKey<$EParam2, Key> + Sync,
            $EParam1: EncryptRef<C1, Key> + Clone + Send,
            $EParam2: EncryptRef<C2, Key> + Clone + Send,
            Key: Sync
        {
            #[inline]
            fn encrypt_ref(value: &$TheStruct<C1, C2>, key: &Key) -> Self {
                let c1 = &value.$field1;
                let c2 = &value.$field2;
                let (e1, e2) = rayon::join(|| c1.encrypt_into(key), || c2.encrypt_into(key));
                $TheStruct::<$EParam1, $EParam2> {
                    $field1: e1,
                    $field2: e2,
                }
            }
        }
    };
}
#[allow(unused_imports)]
pub(crate) use drv2_encrypt2;

#[allow(unused_macros)]
macro_rules! drv3_encrypt {
    ( 
        $TheStruct:ident<$EParam:ident>
        {$f1:ident:$ty1:ty, $f2:ident:$ty2:ty, $f3:ident:$ty3:ty}
    ) => {
        impl<$EParam, C, Key> EncryptRef<$TheStruct<C>, Key> for $TheStruct<$EParam>
        where
            C: DefaultIntoWithKey<$EParam, Key> + Sync,
            $EParam: EncryptRef<C, Key> + Clone + Send,
            Key: Sync
        {
            #[inline]
            fn encrypt_ref(value: &$TheStruct<C>, key: &Key) -> Self {
                use crate::utils::rayon::rayon_join3;
                let c1 = &value.$f1;
                let c2 = &value.$f2;
                let c3 = &value.$f3;
                let (e1, e2, e3) = rayon_join3(|| c1.encrypt_into(key), || c2.encrypt_into(key), || c3.encrypt_into(key));
                $TheStruct::<$EParam> {
                    $f1: e1,
                    $f2: e2,
                    $f3: e3,
                }
            }
        }
    };
}
#[allow(unused_imports)]
pub(crate) use drv3_encrypt;

#[allow(unused_macros)]
macro_rules! drv3_encrypt2 {
    ( 
        $TheStruct:ident<$EParam1:ident, $EParam2:ident>
        {$field1:ident:$type1:ty, $field2:ident:$type2:ty, $field3:ident:$type3:ty}
    ) => {
        impl<$EParam1, $EParam2, C1, C2, Key> EncryptRef<$TheStruct<C1, C2>, Key> for $TheStruct<$EParam1, $EParam2>
        where
            C1: DefaultIntoWithKey<$EParam1, Key> + Sync,
            C2: DefaultIntoWithKey<$EParam2, Key> + Sync,
            $EParam1: EncryptRef<C1, Key> + Clone + Send,
            $EParam2: EncryptRef<C2, Key> + Clone + Send,
            Key: Sync
        {
            #[inline]
            fn encrypt_ref(value: &$TheStruct<C1, C2>, key: &Key) -> Self {
                use crate::utils::rayon::rayon_join3;
                let c1 = &value.$field1;
                let c2 = &value.$field2;
                let c3 = &value.$field3;
                let (e1, e2, e3) = rayon_join3(|| c1.encrypt_into(key), || c2.encrypt_into(key), || c3.encrypt_into(key));
                $TheStruct::<$EParam1, $EParam2> {
                    $field1: e1,
                    $field2: e2,
                    $field3: e3,
                }
            }
        }
    };
}
#[allow(unused_imports)]
pub(crate) use drv3_encrypt2;

#[allow(unused_macros)]
macro_rules! drv4_encrypt {
    ( 
        $TheStruct:ident<$EParam:ident>
        {$f1:ident:$ty1:ty, $f2:ident:$ty2:ty, $f3:ident:$ty3:ty, $f4:ident:$ty4:ty}
    ) => {
        impl<$EParam, C, Key> EncryptRef<$TheStruct<C>, Key> for $TheStruct<$EParam>
        where
            C: DefaultIntoWithKey<$EParam, Key> + Sync,
            $EParam: EncryptRef<C, Key> + Clone + Send,
            Key: Sync
        {
            #[inline]
            fn encrypt_ref(value: &$TheStruct<C>, key: &Key) -> Self {
                use crate::utils::rayon::rayon_join4;
                let c1 = &value.$f1;
                let c2 = &value.$f2;
                let c3 = &value.$f3;
                let c4 = &value.$f4;
                let (e1, e2, e3, e4) = rayon_join4(|| c1.encrypt_into(key), || c2.encrypt_into(key), || c3.encrypt_into(key), || c4.encrypt_into(key));
                $TheStruct::<$EParam> {
                    $f1: e1,
                    $f2: e2,
                    $f3: e3,
                    $f4: e4,
                }
            }
        }
    };
}
#[allow(unused_imports)]
pub(crate) use drv4_encrypt;

#[allow(unused_macros)]
macro_rules! drv4_encrypt2 {
    ( 
        $TheStruct:ident<$EParam1:ident, $EParam2:ident>
        {$f1:ident:$type1:ty, $f2:ident:$type2:ty, $f3:ident:$type3:ty, $f4:ident:$type4:ty}
    ) => {
        impl<$EParam1, $EParam2, C1, C2, Key> EncryptRef<$TheStruct<C1, C2>, Key> for $TheStruct<$EParam1, $EParam2>
        where
            C1: DefaultIntoWithKey<$EParam1, Key> + Sync,
            C2: DefaultIntoWithKey<$EParam2, Key> + Sync,
            $EParam1: EncryptRef<C1, Key> + Clone + Send,
            $EParam2: EncryptRef<C2, Key> + Clone + Send,
            Key: Sync
        {
            #[inline]
            fn encrypt_ref(value: &$TheStruct<C1, C2>, key: &Key) -> Self {
                use crate::utils::rayon::rayon_join4;
                let c1 = &value.$f1;
                let c2 = &value.$f2;
                let c3 = &value.$f3;
                let c4 = &value.$f4;
                let (e1, e2, e3, e4) = rayon_join4(|| c1.encrypt_into(key), || c2.encrypt_into(key), || c3.encrypt_into(key), || c4.encrypt_into(key));
                $TheStruct::<$EParam1, $EParam2> {
                    $f1: e1,
                    $f2: e2,
                    $f3: e3,
                    $f4: e4,
                }
            }
        }
    };
}
#[allow(unused_imports)]
pub(crate) use drv4_encrypt2;

#[allow(unused_macros)]
macro_rules! drv_no_encrypt {
    ( 
        $TheStruct:ident<$EParam:ident>
    ) => {
        impl<$EParam, C, Key> EncryptRef<$TheStruct<C>, Key> for $TheStruct<$EParam>
        where
            $TheStruct<C>: DefaultIntoWithKey<$TheStruct<$EParam>, Key>,
        {
            #[inline]
            fn encrypt_ref(_: &$TheStruct<C>, key: &Key) -> Self {
                <$TheStruct<C> as DefaultIntoWithKey<$TheStruct<$EParam>, Key>>::default_into_with_key(key)
            }
        }
    };
}
#[allow(unused_imports)]
pub(crate) use drv_no_encrypt;

////////////////////////////////////////////////////////////////////////////////
// DECRYPT
////////////////////////////////////////////////////////////////////////////////

impl Decrypt<ClientKey> for FheBool
{
    type Output = bool;
    #[inline]
    fn decrypt(&self, key: &ClientKey) -> bool {
        tfhe::prelude::FheDecrypt::decrypt(self, key)
    }
}

impl Decrypt<ClientKey> for FheUint8
{
    type Output = u8;
    #[inline]
    fn decrypt(&self, key: &ClientKey) -> u8 {
        tfhe::prelude::FheDecrypt::decrypt(self, key)
    }
}

impl<E, Key> Decrypt<Key> for Vec<E>
where
    E: Decrypt<Key> + Sync + DefaultInto<<E as Decrypt<Key>>::Output>,
    <E as Decrypt<Key>>::Output: Send + Clone,
    Key: Sync
 {
    type Output = Vec<<E as Decrypt<Key>>::Output>;
    fn decrypt(&self, key: &Key) -> Self::Output {
        let def = <E as DefaultInto<<E as Decrypt<Key>>::Output>>::default_into();
        let mut result = vec![def; self.len()];
        result
            .par_iter_mut()
            .zip(self.par_iter())
            .for_each(|(dst, v)| *dst = v.decrypt(key));
        result
    }
}

impl<E, Key> Decrypt<Key> for (E,)
where
    E: Decrypt<Key>,
{
    type Output = (<E as Decrypt<Key>>::Output,);
    fn decrypt(&self, key: &Key) -> Self::Output {
        (self.0.decrypt(key),)
    }
}

impl<E, Key, const N: usize> Decrypt<Key> for Box<[E; N]>
where
    E: Decrypt<Key> + Sync + DefaultInto<<E as Decrypt<Key>>::Output>,
    <E as Decrypt<Key>>::Output: Send,
    Key: Sync
 {
    type Output = Box<[<E as Decrypt<Key>>::Output; N]>;
    fn decrypt(&self, key: &Key) -> Self::Output {
        let mut box_result = Box::<[E; N]>::default_into();
        box_result
            .par_iter_mut()
            .zip(self.par_iter())
            .for_each(|(dst, v)| *dst = v.decrypt(key));
        box_result
    }
}

////////////////////////////////////////////////////////////////////////////////
// DECRYPT MACROS
////////////////////////////////////////////////////////////////////////////////

#[allow(unused_macros)]
macro_rules! drv_wrapper_decrypt {
    ( 
        $TheStruct:ident<$EParam:ident>
    ) => {
        impl<$EParam, Key> Decrypt<Key> for $TheStruct<$EParam>
        where
            $EParam: Decrypt<Key> + Sync + DefaultInto<<$EParam as Decrypt<Key>>::Output>,
            <$EParam as Decrypt<Key>>::Output: Send + Clone,
            Key: Sync
        {
            type Output = $TheStruct<<$EParam as Decrypt<Key>>::Output>;
            #[inline]
            fn decrypt(&self, key: &Key) -> Self::Output {
                $TheStruct::<<$EParam as Decrypt<Key>>::Output> (
                    self.0.decrypt(key)
                )
            }
        }
    };
}
#[allow(unused_imports)]
pub(crate) use drv_wrapper_decrypt;

#[allow(unused_macros)]
macro_rules! drv1_decrypt {
    ( 
        $TheStruct:ident<$EParam:ident>
        {$f1:ident:$ty1:ty}
    ) => {
        impl<$EParam, Key> Decrypt<Key> for $TheStruct<$EParam>
        where
            $EParam: Decrypt<Key> + Sync + DefaultInto<<$EParam as Decrypt<Key>>::Output>,
            <$EParam as Decrypt<Key>>::Output: Send + Clone,
            Key: Sync
            {
            type Output = $TheStruct<<$EParam as Decrypt<Key>>::Output>;
            #[inline]
            fn decrypt(&self, key: &Key) -> Self::Output {
                Self::Output {
                    $f1: self.$f1.decrypt(key)
                }
            }
        }
    };
}
#[allow(unused_imports)]
pub(crate) use drv1_decrypt;

#[allow(unused_macros)]
macro_rules! drv1_decrypt2 {
    ( 
        $TheStruct:ident<$EParam1:ident, $EParam2:ident>
        {$f1:ident:$ty1:ty}
    ) => {
        impl<$EParam1, $EParam2, Key> Decrypt<Key> for $TheStruct<$EParam1, $EParam2>
        where
            $EParam1: Decrypt<Key> + Sync + DefaultInto<<$EParam1 as Decrypt<Key>>::Output>,
            $EParam2: Decrypt<Key> + Sync + DefaultInto<<$EParam2 as Decrypt<Key>>::Output>,
            <$EParam1 as Decrypt<Key>>::Output: Send + Clone,
            <$EParam2 as Decrypt<Key>>::Output: Send + Clone,
            Key: Sync
        {
            type Output = $TheStruct<<$EParam1 as Decrypt<Key>>::Output, <$EParam2 as Decrypt<Key>>::Output>;
            #[inline]
            fn decrypt(&self, key: &Key) -> Self::Output {
                Self::Output {
                    $f1: self.$f1.decrypt(key)
                }
            }
        }
    };
}
#[allow(unused_imports)]
pub(crate) use drv1_decrypt2;

#[allow(unused_macros)]
macro_rules! drv2_decrypt {
    ( 
        $TheStruct:ident<$EParam:ident>
        {$f1:ident:$ty1:ty, $f2:ident:$ty2:ty}
    ) => {
        impl<$EParam, Key> Decrypt<Key> for $TheStruct<$EParam>
        where
            $EParam: Decrypt<Key> + Sync + DefaultInto<<$EParam as Decrypt<Key>>::Output>,
            <$EParam as Decrypt<Key>>::Output: Send + Clone,
            Key: Sync
            {
            type Output = $TheStruct<<$EParam as Decrypt<Key>>::Output>;
            #[inline]
            fn decrypt(&self, key: &Key) -> Self::Output {
                let c1 = &self.$f1;
                let c2 = &self.$f2;
                let (e1, e2) = rayon::join(|| c1.decrypt(key), || c2.decrypt(key));
                Self::Output {
                    $f1: e1,
                    $f2: e2,
                }
            }
        }
    };
}
#[allow(unused_imports)]
pub(crate) use drv2_decrypt;

#[allow(unused_macros)]
macro_rules! drv2_decrypt2 {
    ( 
        $TheStruct:ident<$EParam1:ident, $EParam2:ident>
        {$f1:ident:$ty1:ty, $f2:ident:$ty2:ty}
    ) => {
        impl<$EParam1, $EParam2, Key> Decrypt<Key> for $TheStruct<$EParam1, $EParam2>
        where
            $EParam1: Decrypt<Key> + Sync + DefaultInto<<$EParam1 as Decrypt<Key>>::Output>,
            $EParam2: Decrypt<Key> + Sync + DefaultInto<<$EParam2 as Decrypt<Key>>::Output>,
            <$EParam1 as Decrypt<Key>>::Output: Send + Clone,
            <$EParam2 as Decrypt<Key>>::Output: Send + Clone,
            Key: Sync
        {
            type Output = $TheStruct<<$EParam1 as Decrypt<Key>>::Output, <$EParam2 as Decrypt<Key>>::Output>;
            #[inline]
            fn decrypt(&self, key: &Key) -> Self::Output {
                let c1 = &self.$f1;
                let c2 = &self.$f2;
                let (e1, e2) = rayon::join(|| c1.decrypt(key), || c2.decrypt(key));
                Self::Output {
                    $f1: e1,
                    $f2: e2,
                }
            }
        }
    };
}
#[allow(unused_imports)]
pub(crate) use drv2_decrypt2;

#[allow(unused_macros)]
macro_rules! drv3_decrypt {
    ( 
        $TheStruct:ident<$EParam:ident>
        {$f1:ident:$ty1:ty, $f2:ident:$ty2:ty, $f3:ident:$ty3:ty}
    ) => {
        impl<$EParam, Key> Decrypt<Key> for $TheStruct<$EParam>
        where
            $EParam: Decrypt<Key> + Sync + DefaultInto<<$EParam as Decrypt<Key>>::Output>,
            <$EParam as Decrypt<Key>>::Output: Send + Clone,
            Key: Sync
            {
            type Output = $TheStruct<<$EParam as Decrypt<Key>>::Output>;
            #[inline]
            fn decrypt(&self, key: &Key) -> Self::Output {
                use crate::utils::rayon::rayon_join3;
                let c1 = &self.$f1;
                let c2 = &self.$f2;
                let c3 = &self.$f3;
                let (e1, e2, e3) = rayon_join3(|| c1.decrypt(key), || c2.decrypt(key), || c3.decrypt(key));
                Self::Output {
                    $f1: e1,
                    $f2: e2,
                    $f3: e3,
                }
            }
        }
    };
}
#[allow(unused_imports)]
pub(crate) use drv3_decrypt;

#[allow(unused_macros)]
macro_rules! drv3_decrypt2 {
    ( 
        $TheStruct:ident<$EParam1:ident, $EParam2:ident>
        {$f1:ident:$ty1:ty, $f2:ident:$ty2:ty, $f3:ident:$ty3:ty}
    ) => {
        impl<$EParam1, $EParam2, Key> Decrypt<Key> for $TheStruct<$EParam1, $EParam2>
        where
            $EParam1: Decrypt<Key> + Sync + DefaultInto<<$EParam1 as Decrypt<Key>>::Output>,
            $EParam2: Decrypt<Key> + Sync + DefaultInto<<$EParam2 as Decrypt<Key>>::Output>,
            <$EParam1 as Decrypt<Key>>::Output: Send + Clone,
            <$EParam2 as Decrypt<Key>>::Output: Send + Clone,
            Key: Sync
        {
            type Output = $TheStruct<<$EParam1 as Decrypt<Key>>::Output, <$EParam2 as Decrypt<Key>>::Output>;
            #[inline]
            fn decrypt(&self, key: &Key) -> Self::Output {
                use crate::utils::rayon::rayon_join3;
                let c1 = &self.$f1;
                let c2 = &self.$f2;
                let c3 = &self.$f3;
                let (e1, e2, e3) = rayon_join3(|| c1.decrypt(key), || c2.decrypt(key), || c3.decrypt(key));
                Self::Output {
                    $f1: e1,
                    $f2: e2,
                    $f3: e3,
                }
            }
        }
    };
}
#[allow(unused_imports)]
pub(crate) use drv3_decrypt2;

#[allow(unused_macros)]
macro_rules! drv4_decrypt {
    ( 
        $TheStruct:ident<$EParam:ident>
        {$f1:ident:$ty1:ty, $f2:ident:$ty2:ty, $f3:ident:$ty3:ty, $f4:ident:$ty4:ty}
    ) => {
        impl<$EParam, Key> Decrypt<Key> for $TheStruct<$EParam>
        where
            $EParam: Decrypt<Key> + Sync + DefaultInto<<$EParam as Decrypt<Key>>::Output>,
            <$EParam as Decrypt<Key>>::Output: Send + Clone,
            Key: Sync
            {
            type Output = $TheStruct<<$EParam as Decrypt<Key>>::Output>;
            #[inline]
            fn decrypt(&self, key: &Key) -> Self::Output {
                use crate::utils::rayon::rayon_join4;
                let c1 = &self.$f1;
                let c2 = &self.$f2;
                let c3 = &self.$f3;
                let c4 = &self.$f4;
                let (e1, e2, e3, e4) = rayon_join4(|| c1.decrypt(key), || c2.decrypt(key), || c3.decrypt(key), || c4.decrypt(key));
                Self::Output {
                    $f1: e1,
                    $f2: e2,
                    $f3: e3,
                    $f4: e4,
                }
            }
        }
    };
}
#[allow(unused_imports)]
pub(crate) use drv4_decrypt;

#[allow(unused_macros)]
macro_rules! drv4_decrypt2 {
    ( 
        $TheStruct:ident<$EParam1:ident, $EParam2:ident>
        {$f1:ident:$ty1:ty, $f2:ident:$ty2:ty, $f3:ident:$ty3:ty, $f4:ident:$ty4:ty}
    ) => {
        impl<$EParam1, $EParam2, Key> Decrypt<Key> for $TheStruct<$EParam1, $EParam2>
        where
            $EParam1: Decrypt<Key> + Sync + DefaultInto<<$EParam1 as Decrypt<Key>>::Output>,
            $EParam2: Decrypt<Key> + Sync + DefaultInto<<$EParam2 as Decrypt<Key>>::Output>,
            <$EParam1 as Decrypt<Key>>::Output: Send + Clone,
            <$EParam2 as Decrypt<Key>>::Output: Send + Clone,
            Key: Sync
        {
            type Output = $TheStruct<<$EParam1 as Decrypt<Key>>::Output, <$EParam2 as Decrypt<Key>>::Output>;
            #[inline]
            fn decrypt(&self, key: &Key) -> Self::Output {
                use crate::utils::rayon::rayon_join4;
                let c1 = &self.$f1;
                let c2 = &self.$f2;
                let c3 = &self.$f3;
                let c4 = &self.$f4;
                let (e1, e2, e3, e4) = rayon_join4(|| c1.decrypt(key), || c2.decrypt(key), || c3.decrypt(key), || c4.decrypt(key));
                Self::Output {
                    $f1: e1,
                    $f2: e2,
                    $f3: e3,
                    $f4: e4,
                }
            }
        }
    };
}
#[allow(unused_imports)]
pub(crate) use drv4_decrypt2;

////////////////////////////////////////////////////////////////////////////////
// TRIVIAL ENCRYPT
////////////////////////////////////////////////////////////////////////////////

// Must redefine our own traits to add encrypt/decrypt support on Vec
impl TrivialEncryptRef<bool> for FheBool
where
    FheBool: FheTrivialEncrypt<bool>,
{
    #[inline]
    fn encrypt_trivial_ref(value: &bool) -> Self {
        <FheBool as FheTrivialEncrypt<bool>>::encrypt_trivial(*value)
    }
}

impl TrivialEncryptRef<u8> for FheUint8
where
    FheUint8: FheTrivialEncrypt<u8>,
{
    #[inline]
    fn encrypt_trivial_ref(value: &u8) -> Self {
        <FheUint8 as FheTrivialEncrypt<u8>>::encrypt_trivial(*value)
    }
}

impl<E, C> TrivialEncryptRef<Vec<C>> for Vec<E>
where
    E: TrivialEncryptRef<C> + Clone + Send,
    C: DefaultInto<E> + Sync,
{
    fn encrypt_trivial_ref(value: &Vec<C>) -> Self {
        let def = C::default_into();
        let mut result = vec![def; value.len()];
        result
            .par_iter_mut()
            .zip(value.par_iter())
            .for_each(|(dst, v)| *dst = E::encrypt_trivial_ref(v));
        result
    }
}

impl<E, C> TrivialEncryptRef<(C,)> for (E,)
where
    E: TrivialEncryptRef<C>
{
    fn encrypt_trivial_ref(value: &(C,)) -> Self {
        (E::encrypt_trivial_ref(&value.0),)
    }
}

impl<E, C, const N: usize> TrivialEncryptRef<Box<[C; N]>> for Box<[E; N]>
where
    E: TrivialEncryptRef<C> + Send,
    C: DefaultInto<E> + Sync,
{
    fn encrypt_trivial_ref(value: &Box<[C; N]>) -> Self {
        let mut box_result = Box::<[C; N]>::default_into();
        box_result
            .par_iter_mut()
            .zip(value.par_iter())
            .for_each(|(dst, v)| *dst = E::encrypt_trivial_ref(v));
        box_result
    }
}

////////////////////////////////////////////////////////////////////////////////
// TRIVIAL ENCRYPT MACROS
////////////////////////////////////////////////////////////////////////////////

#[allow(unused_macros)]
macro_rules! drv_wrapper_trivial_encrypt {
    ( 
        $TheStruct:ident<$EParam:ident>
    ) => {
        impl<C, $EParam> TrivialEncryptRef<$TheStruct<C>> for $TheStruct<$EParam>
        where
            C: DefaultInto<$EParam> + Sync,
            $EParam: TrivialEncryptRef<C> + Clone + Send
        {
            #[inline]
            fn encrypt_trivial_ref(value: &$TheStruct<C>) -> Self {
                $TheStruct::<$EParam> (
                    value.0.encrypt_trivial_into()
                )
            }
        }
    };
}
#[allow(unused_imports)]
pub(crate) use drv_wrapper_trivial_encrypt;

#[allow(unused_macros)]
macro_rules! drv1_trivial_encrypt {
    ( 
        $TheStruct:ident<$EParam:ident>
        {$f1:ident:$ty1:ty}
    ) => {
        impl<C, $EParam> TrivialEncryptRef<$TheStruct<C>> for $TheStruct<$EParam>
        where
            C: DefaultInto<$EParam> + Sync,
            $EParam: TrivialEncryptRef<C> + Clone + Send
        {
            #[inline]
            fn encrypt_trivial_ref(value: &$TheStruct<C>) -> Self {
                $TheStruct::<$EParam> {
                    $f1: value.$f1.encrypt_trivial_into()
                }
            }
        }
    };
}
#[allow(unused_imports)]
pub(crate) use drv1_trivial_encrypt;

#[allow(unused_macros)]
macro_rules! drv1_trivial_encrypt2 {
    ( 
        $TheStruct:ident<$EParam1:ident, $EParam2:ident>
        {$f1:ident:$ty1:ty}
    ) => {
        impl<C1, C2, $EParam1, $EParam2> TrivialEncryptRef<$TheStruct<C1, C2>> for $TheStruct<$EParam1, $EParam2>
        where
            C1: DefaultInto<$EParam1> + Sync,
            C2: DefaultInto<$EParam2> + Sync,
            $EParam1: TrivialEncryptRef<C1> + Clone + Send,
            $EParam2: TrivialEncryptRef<C2> + Clone + Send
        {
            #[inline]
            fn encrypt_trivial_ref(value: &$TheStruct<C1, C2>) -> Self {
                $TheStruct::<$EParam1, $EParam2> {
                    $f1: value.$f1.encrypt_trivial_into()
                }
            }
        }
    };
}
#[allow(unused_imports)]
pub(crate) use drv1_trivial_encrypt2;

#[allow(unused_macros)]
macro_rules! drv2_trivial_encrypt {
    ( 
        $TheStruct:ident<$EParam:ident>
        {$f1:ident:$ty1:ty, $f2:ident:$ty2:ty}
    ) => {
        impl<C, $EParam> TrivialEncryptRef<$TheStruct<C>> for $TheStruct<$EParam>
        where
            C: DefaultInto<$EParam> + Sync,
            $EParam: TrivialEncryptRef<C> + Clone + Send
        {
            #[inline]
            fn encrypt_trivial_ref(value: &$TheStruct<C>) -> Self {
                let c1 = &value.$f1;
                let c2 = &value.$f2;
                let (e1, e2) = rayon::join(|| c1.encrypt_trivial_into(), || c2.encrypt_trivial_into());
                $TheStruct::<$EParam> {
                    $f1: e1,
                    $f2: e2,
                }
            }
        }
    };
}
#[allow(unused_imports)]
pub(crate) use drv2_trivial_encrypt;

#[allow(unused_macros)]
macro_rules! drv2_trivial_encrypt2 {
    ( 
        $TheStruct:ident<$EParam1:ident, $EParam2:ident>
        {$f1:ident:$ty1:ty, $f2:ident:$ty2:ty}
    ) => {
        impl<C1, C2, $EParam1, $EParam2> TrivialEncryptRef<$TheStruct<C1, C2>> for $TheStruct<$EParam1, $EParam2>
        where
            C1: DefaultInto<$EParam1> + Sync,
            C2: DefaultInto<$EParam2> + Sync,
            $EParam1: TrivialEncryptRef<C1> + Clone + Send,
            $EParam2: TrivialEncryptRef<C2> + Clone + Send
        {
            #[inline]
            fn encrypt_trivial_ref(value: &$TheStruct<C1, C2>) -> Self {
                let c1 = &value.$f1;
                let c2 = &value.$f2;
                let (e1, e2) = rayon::join(|| c1.encrypt_trivial_into(), || c2.encrypt_trivial_into());
                $TheStruct::<$EParam1, $EParam2> {
                    $f1: e1,
                    $f2: e2,
                }
            }
        }
    };
}
#[allow(unused_imports)]
pub(crate) use drv2_trivial_encrypt2;

#[allow(unused_macros)]
macro_rules! drv3_trivial_encrypt {
    ( 
        $TheStruct:ident<$EParam:ident>
        {$f1:ident:$ty1:ty, $f2:ident:$ty2:ty, $f3:ident:$ty3:ty}
    ) => {
        impl<C, $EParam> TrivialEncryptRef<$TheStruct<C>> for $TheStruct<$EParam>
        where
            C: DefaultInto<$EParam> + Sync,
            $EParam: TrivialEncryptRef<C> + Clone + Send
        {
            #[inline]
            fn encrypt_trivial_ref(value: &$TheStruct<C>) -> Self {
                use crate::utils::rayon::rayon_join3;
                let c1 = &value.$f1;
                let c2 = &value.$f2;
                let c3 = &value.$f3;
                let (e1, e2, e3) = rayon_join3(|| c1.encrypt_trivial_into(), || c2.encrypt_trivial_into(), || c3.encrypt_trivial_into());
                $TheStruct::<$EParam> {
                    $f1: e1,
                    $f2: e2,
                    $f3: e3,
                }
            }
        }
    };
}
#[allow(unused_imports)]
pub(crate) use drv3_trivial_encrypt;

#[allow(unused_macros)]
macro_rules! drv3_trivial_encrypt2 {
    ( 
        $TheStruct:ident<$EParam1:ident, $EParam2:ident>
        {$f1:ident:$ty1:ty, $f2:ident:$ty2:ty, $f3:ident:$ty3:ty}
    ) => {
        impl<C1, C2, $EParam1, $EParam2> TrivialEncryptRef<$TheStruct<C1, C2>> for $TheStruct<$EParam1, $EParam2>
        where
            C1: DefaultInto<$EParam1> + Sync,
            C2: DefaultInto<$EParam2> + Sync,
            $EParam1: TrivialEncryptRef<C1> + Clone + Send,
            $EParam2: TrivialEncryptRef<C2> + Clone + Send
        {
            #[inline]
            fn encrypt_trivial_ref(value: &$TheStruct<C1, C2>) -> Self {
                use crate::utils::rayon::rayon_join3;
                let c1 = &value.$f1;
                let c2 = &value.$f2;
                let c3 = &value.$f3;
                let (e1, e2, e3) = rayon_join3(|| c1.encrypt_trivial_into(), || c2.encrypt_trivial_into(), || c3.encrypt_trivial_into());
                $TheStruct::<$EParam1, $EParam2> {
                    $f1: e1,
                    $f2: e2,
                    $f3: e3,
                }
            }
        }
    };
}
#[allow(unused_imports)]
pub(crate) use drv3_trivial_encrypt2;

#[allow(unused_macros)]
macro_rules! drv4_trivial_encrypt {
    ( 
        $TheStruct:ident<$EParam:ident>
        {$f1:ident:$ty1:ty, $f2:ident:$ty2:ty, $f3:ident:$ty3:ty, $f4:ident:$ty4:ty}
    ) => {
        impl<C, $EParam> TrivialEncryptRef<$TheStruct<C>> for $TheStruct<$EParam>
        where
            C: DefaultInto<$EParam> + Sync,
            $EParam: TrivialEncryptRef<C> + Clone + Send
        {
            #[inline]
            fn encrypt_trivial_ref(value: &$TheStruct<C>) -> Self {
                use crate::utils::rayon::rayon_join4;
                let c1 = &value.$f1;
                let c2 = &value.$f2;
                let c3 = &value.$f3;
                let c4 = &value.$f4;
                let (e1, e2, e3, e4) = rayon_join4(|| c1.encrypt_trivial_into(), || c2.encrypt_trivial_into(), || c3.encrypt_trivial_into(), || c4.encrypt_trivial_into());
                $TheStruct::<$EParam> {
                    $f1: e1,
                    $f2: e2,
                    $f3: e3,
                    $f4: e4,
                }
            }
        }
    };
}
#[allow(unused_imports)]
pub(crate) use drv4_trivial_encrypt;

#[allow(unused_macros)]
macro_rules! drv4_trivial_encrypt2 {
    ( 
        $TheStruct:ident<$EParam1:ident, $EParam2:ident>
        {$f1:ident:$ty1:ty, $f2:ident:$ty2:ty, $f3:ident:$ty3:ty, $f4:ident:$ty4:ty}
    ) => {
        impl<C1, C2, $EParam1, $EParam2> TrivialEncryptRef<$TheStruct<C1, C2>> for $TheStruct<$EParam1, $EParam2>
        where
            C1: DefaultInto<$EParam1> + Sync,
            C2: DefaultInto<$EParam2> + Sync,
            $EParam1: TrivialEncryptRef<C1> + Clone + Send,
            $EParam2: TrivialEncryptRef<C2> + Clone + Send
        {
            #[inline]
            fn encrypt_trivial_ref(value: &$TheStruct<C1, C2>) -> Self {
                use crate::utils::rayon::rayon_join4;
                let c1 = &value.$f1;
                let c2 = &value.$f2;
                let c3 = &value.$f3;
                let c4 = &value.$f4;
                let (e1, e2, e3, e4) = rayon_join4(|| c1.encrypt_trivial_into(), || c2.encrypt_trivial_into(), || c3.encrypt_trivial_into(), || c4.encrypt_trivial_into());
                $TheStruct::<$EParam1, $EParam2> {
                    $f1: e1,
                    $f2: e2,
                    $f3: e3,
                    $f4: e4,
                }
            }
        }
    };
}
#[allow(unused_imports)]
pub(crate) use drv4_trivial_encrypt2;

#[allow(unused_macros)]
macro_rules! drv_no_trivial_encrypt {
    ( 
        $TheStruct:ident<$EParam:ident>
    ) => {
        impl<$EParam, C> TrivialEncryptRef<$TheStruct<C>> for $TheStruct<$EParam>
        where
            $TheStruct<C>: DefaultInto<$TheStruct<$EParam>>,
        {
            #[inline]
            fn encrypt_trivial_ref(_: &$TheStruct<C>) -> Self {
                <$TheStruct<C> as DefaultInto<$TheStruct<$EParam>>>::default_into()
            }
        }
    };
}
#[allow(unused_imports)]
pub(crate) use drv_no_trivial_encrypt;

////////////////////////////////////////////////////////////////////////////////
// TRY TRIVIAL DECRYPT
////////////////////////////////////////////////////////////////////////////////

impl TryTrivialDecrypt for FheBool
{
    type Output = bool;
    #[inline]
    fn try_decrypt_trivial(&self) -> Result<Self::Output, tfhe::shortint::ciphertext::NotTrivialCiphertextError> {
        FheBool::try_decrypt_trivial(self)
    }
}

impl TryTrivialDecrypt for FheUint8
{
    type Output = u8;
    #[inline]
    fn try_decrypt_trivial(&self) -> Result<Self::Output, tfhe::shortint::ciphertext::NotTrivialCiphertextError> {
        FheUint8::try_decrypt_trivial(self)
    }
}

impl<E> TryTrivialDecrypt for Vec<E>
where
    E: TryTrivialDecrypt + Sync + DefaultInto<<E as TryTrivialDecrypt>::Output>,
    <E as TryTrivialDecrypt>::Output: Send + Clone,
 {
    type Output = Vec<<E as TryTrivialDecrypt>::Output>;
    fn try_decrypt_trivial(&self) -> Result<Self::Output, tfhe::shortint::ciphertext::NotTrivialCiphertextError> {
        let def = <E as DefaultInto<<E as TryTrivialDecrypt>::Output>>::default_into();
        let mut result = vec![def; self.len()];
        result
            .par_iter_mut()
            .zip(self.par_iter())
            .try_for_each(|(dst, v)| match v.try_decrypt_trivial() {
                Ok(clear) => {
                    *dst = clear;
                    Ok(())
                }
                Err(e) => Err(e),
            })?;
        Ok(result)
    }
}

impl<E> TryTrivialDecrypt for (E,)
where
    E: TryTrivialDecrypt,
{
    type Output = (<E as TryTrivialDecrypt>::Output,);
    fn try_decrypt_trivial(&self) -> Result<Self::Output, tfhe::shortint::ciphertext::NotTrivialCiphertextError> {
        Ok((self.0.try_decrypt_trivial()?,))
    }
}

impl<E, const N: usize> TryTrivialDecrypt for Box<[E; N]>
where
    E: TryTrivialDecrypt + Sync + DefaultInto<<E as TryTrivialDecrypt>::Output>,
    <E as TryTrivialDecrypt>::Output: Send,
 {
    type Output = Box<[<E as TryTrivialDecrypt>::Output; N]>;
    fn try_decrypt_trivial(&self) -> Result<Self::Output, tfhe::shortint::ciphertext::NotTrivialCiphertextError> {
        let mut box_result = Box::<[E; N]>::default_into();
        box_result
            .par_iter_mut()
            .zip(self.par_iter())
            .try_for_each(|(dst, v)| match v.try_decrypt_trivial() {
                Ok(clear) => {
                    *dst = clear;
                    Ok(())
                }
                Err(e) => Err(e),
            })?;
        Ok(box_result)
    }
}

////////////////////////////////////////////////////////////////////////////////
// TRY TRIVIAL DECRYPT MACROS
////////////////////////////////////////////////////////////////////////////////

#[allow(unused_macros)]
macro_rules! drv_wrapper_try_trivial_decrypt {
    ( 
        $TheStruct:ident<$EParam:ident>
    ) => {
        impl<$EParam> TryTrivialDecrypt for $TheStruct<$EParam>
        where
            $EParam: TryTrivialDecrypt + Sync + DefaultInto<<$EParam as TryTrivialDecrypt>::Output>,
            <$EParam as TryTrivialDecrypt>::Output: Send + Clone,
        {
            type Output = $TheStruct<<$EParam as TryTrivialDecrypt>::Output>;
            #[inline]
            fn try_decrypt_trivial(
                &self,
            ) -> Result<Self::Output, tfhe::shortint::ciphertext::NotTrivialCiphertextError> {
                Ok($TheStruct::<<$EParam as TryTrivialDecrypt>::Output> (
                    self.0.try_decrypt_trivial()?,
                ))
            }
        }
    };
}
#[allow(unused_imports)]
pub(crate) use drv_wrapper_try_trivial_decrypt;

#[allow(unused_macros)]
macro_rules! drv1_try_trivial_decrypt {
    ( 
        $TheStruct:ident<$EParam:ident>
        {$f1:ident:$ty1:ty}
    ) => {
        impl<$EParam> TryTrivialDecrypt for $TheStruct<$EParam>
        where
            $EParam: TryTrivialDecrypt + Sync + DefaultInto<<$EParam as TryTrivialDecrypt>::Output>,
            <$EParam as TryTrivialDecrypt>::Output: Send + Clone,
        {
            type Output = $TheStruct<<$EParam as TryTrivialDecrypt>::Output>;
            #[inline]
            fn try_decrypt_trivial(
                &self,
            ) -> Result<Self::Output, tfhe::shortint::ciphertext::NotTrivialCiphertextError> {
                Ok(Self::Output {
                    $f1: self.$f1.try_decrypt_trivial()?,
                })
            }
        }
    };
}
#[allow(unused_imports)]
pub(crate) use drv1_try_trivial_decrypt;

#[allow(unused_macros)]
macro_rules! drv1_try_trivial_decrypt2 {
    ( 
        $TheStruct:ident<$EParam1:ident, $EParam2:ident>
        {$f1:ident:$ty1:ty}
    ) => {
        impl<$EParam1, $EParam2> TryTrivialDecrypt for $TheStruct<$EParam1, $EParam2>
        where
            $EParam1: TryTrivialDecrypt + Sync + DefaultInto<<$EParam1 as TryTrivialDecrypt>::Output>,
            <$EParam1 as TryTrivialDecrypt>::Output: Send + Clone,
            $EParam2: TryTrivialDecrypt + Sync + DefaultInto<<$EParam2 as TryTrivialDecrypt>::Output>,
            <$EParam2 as TryTrivialDecrypt>::Output: Send + Clone,
        {
            type Output = $TheStruct<<$EParam1 as TryTrivialDecrypt>::Output, <$EParam2 as TryTrivialDecrypt>::Output>;
            #[inline]
            fn try_decrypt_trivial(
                &self,
            ) -> Result<Self::Output, tfhe::shortint::ciphertext::NotTrivialCiphertextError> {
                Ok(Self::Output {
                    $f1: self.$f1.try_decrypt_trivial()?,
                })
            }
        }
    };
}
#[allow(unused_imports)]
pub(crate) use drv1_try_trivial_decrypt2;

#[allow(unused_macros)]
macro_rules! drv2_try_trivial_decrypt {
    ( 
        $TheStruct:ident<$EParam:ident>
        {$f1:ident:$ty1:ty, $f2:ident:$ty2:ty}
    ) => {
        impl<$EParam> TryTrivialDecrypt for $TheStruct<$EParam>
        where
            $EParam: TryTrivialDecrypt + Sync + DefaultInto<<$EParam as TryTrivialDecrypt>::Output>,
            <$EParam as TryTrivialDecrypt>::Output: Send + Clone,
        {
            type Output = $TheStruct<<$EParam as TryTrivialDecrypt>::Output>;
            #[inline]
            fn try_decrypt_trivial(
                &self,
            ) -> Result<Self::Output, tfhe::shortint::ciphertext::NotTrivialCiphertextError> {
                let e0 = &self.$f1;
                let e1 = &self.$f2;
                let (d0, d1) =
                    rayon::join(|| e0.try_decrypt_trivial(), || e1.try_decrypt_trivial());
                Ok(Self::Output {
                    $f1: d0?,
                    $f2: d1?,
                })
            }
        }
    };
}
#[allow(unused_imports)]
pub(crate) use drv2_try_trivial_decrypt;

#[allow(unused_macros)]
macro_rules! drv2_try_trivial_decrypt2 {
    ( 
        $TheStruct:ident<$EParam1:ident, $EParam2:ident>
        {$f1:ident:$ty1:ty, $f2:ident:$ty2:ty}
    ) => {
        impl<$EParam1, $EParam2> TryTrivialDecrypt for $TheStruct<$EParam1, $EParam2>
        where
            $EParam1: TryTrivialDecrypt + Sync + DefaultInto<<$EParam1 as TryTrivialDecrypt>::Output>,
            <$EParam1 as TryTrivialDecrypt>::Output: Send + Clone,
            $EParam2: TryTrivialDecrypt + Sync + DefaultInto<<$EParam2 as TryTrivialDecrypt>::Output>,
            <$EParam2 as TryTrivialDecrypt>::Output: Send + Clone,
        {
            type Output = $TheStruct<<$EParam1 as TryTrivialDecrypt>::Output, <$EParam2 as TryTrivialDecrypt>::Output>;
            #[inline]
            fn try_decrypt_trivial(
                &self,
            ) -> Result<Self::Output, tfhe::shortint::ciphertext::NotTrivialCiphertextError> {
                let e0 = &self.$f1;
                let e1 = &self.$f2;
                let (d0, d1) =
                    rayon::join(|| e0.try_decrypt_trivial(), || e1.try_decrypt_trivial());
                Ok(Self::Output {
                    $f1: d0?,
                    $f2: d1?,
                })
            }
        }
    };
}
#[allow(unused_imports)]
pub(crate) use drv2_try_trivial_decrypt2;

#[allow(unused_macros)]
macro_rules! drv3_try_trivial_decrypt {
    ( 
        $TheStruct:ident<$EParam:ident>
        {$f1:ident:$ty1:ty, $f2:ident:$ty2:ty, $f3:ident:$ty3:ty}
    ) => {
        impl<$EParam> TryTrivialDecrypt for $TheStruct<$EParam>
        where
            $EParam: TryTrivialDecrypt + Sync + DefaultInto<<$EParam as TryTrivialDecrypt>::Output>,
            <$EParam as TryTrivialDecrypt>::Output: Send + Clone,
        {
            type Output = $TheStruct<<$EParam as TryTrivialDecrypt>::Output>;
            #[inline]
            fn try_decrypt_trivial(
                &self,
            ) -> Result<Self::Output, tfhe::shortint::ciphertext::NotTrivialCiphertextError> {
                use crate::utils::rayon::rayon_join3;
                let e0 = &self.$f1;
                let e1 = &self.$f2;
                let e3 = &self.$f3;
                let (d0, d1, d2) =
                    rayon_join3(|| e0.try_decrypt_trivial(), || e1.try_decrypt_trivial(), || e3.try_decrypt_trivial());
                Ok(Self::Output {
                    $f1: d0?,
                    $f2: d1?,
                    $f3: d2?,
                })
            }
        }
    };
}
#[allow(unused_imports)]
pub(crate) use drv3_try_trivial_decrypt;

#[allow(unused_macros)]
macro_rules! drv3_try_trivial_decrypt2 {
    ( 
        $TheStruct:ident<$EParam1:ident, $EParam2:ident>
        {$f1:ident:$ty1:ty, $f2:ident:$ty2:ty, $f3:ident:$ty3:ty}
    ) => {
        impl<$EParam1, $EParam2> TryTrivialDecrypt for $TheStruct<$EParam1, $EParam2>
        where
            $EParam1: TryTrivialDecrypt + Sync + DefaultInto<<$EParam1 as TryTrivialDecrypt>::Output>,
            <$EParam1 as TryTrivialDecrypt>::Output: Send + Clone,
            $EParam2: TryTrivialDecrypt + Sync + DefaultInto<<$EParam2 as TryTrivialDecrypt>::Output>,
            <$EParam2 as TryTrivialDecrypt>::Output: Send + Clone,
        {
            type Output = $TheStruct<<$EParam1 as TryTrivialDecrypt>::Output, <$EParam2 as TryTrivialDecrypt>::Output>;
            #[inline]
            fn try_decrypt_trivial(
                &self,
            ) -> Result<Self::Output, tfhe::shortint::ciphertext::NotTrivialCiphertextError> {
                use crate::utils::rayon::rayon_join3;
                let e0 = &self.$f1;
                let e1 = &self.$f2;
                let e3 = &self.$f3;
                let (d0, d1, d2) =
                    rayon_join3(|| e0.try_decrypt_trivial(), || e1.try_decrypt_trivial(), || e3.try_decrypt_trivial());
                Ok(Self::Output {
                    $f1: d0?,
                    $f2: d1?,
                    $f3: d2?,
                })
            }
        }
    };
}
#[allow(unused_imports)]
pub(crate) use drv3_try_trivial_decrypt2;

#[allow(unused_macros)]
macro_rules! drv4_try_trivial_decrypt {
    ( 
        $TheStruct:ident<$EParam:ident>
        {$f1:ident:$ty1:ty, $f2:ident:$ty2:ty, $f3:ident:$ty3:ty, $f4:ident:$ty4:ty}
    ) => {
        impl<$EParam> TryTrivialDecrypt for $TheStruct<$EParam>
        where
            $EParam: TryTrivialDecrypt + Sync + DefaultInto<<$EParam as TryTrivialDecrypt>::Output>,
            <$EParam as TryTrivialDecrypt>::Output: Send + Clone,
        {
            type Output = $TheStruct<<$EParam as TryTrivialDecrypt>::Output>;
            #[inline]
            fn try_decrypt_trivial(
                &self,
            ) -> Result<Self::Output, tfhe::shortint::ciphertext::NotTrivialCiphertextError> {
                use crate::utils::rayon::rayon_join4;
                let e0 = &self.$f1;
                let e1 = &self.$f2;
                let e2 = &self.$f3;
                let e3 = &self.$f4;
                let (d0, d1, d2, d3) =
                    rayon_join4(|| e0.try_decrypt_trivial(), || e1.try_decrypt_trivial(), || e2.try_decrypt_trivial(), || e3.try_decrypt_trivial());
                Ok(Self::Output {
                    $f1: d0?,
                    $f2: d1?,
                    $f3: d2?,
                    $f4: d3?,
                })
            }
        }
    };
}
#[allow(unused_imports)]
pub(crate) use drv4_try_trivial_decrypt;

#[allow(unused_macros)]
macro_rules! drv4_try_trivial_decrypt2 {
    ( 
        $TheStruct:ident<$EParam1:ident, $EParam2:ident>
        {$f1:ident:$ty1:ty, $f2:ident:$ty2:ty, $f3:ident:$ty3:ty, $f4:ident:$ty4:ty}
    ) => {
        impl<$EParam1, $EParam2> TryTrivialDecrypt for $TheStruct<$EParam1, $EParam2>
        where
            $EParam1: TryTrivialDecrypt + Sync + DefaultInto<<$EParam1 as TryTrivialDecrypt>::Output>,
            <$EParam1 as TryTrivialDecrypt>::Output: Send + Clone,
            $EParam2: TryTrivialDecrypt + Sync + DefaultInto<<$EParam2 as TryTrivialDecrypt>::Output>,
            <$EParam2 as TryTrivialDecrypt>::Output: Send + Clone,
        {
            type Output = $TheStruct<<$EParam1 as TryTrivialDecrypt>::Output, <$EParam2 as TryTrivialDecrypt>::Output>;
            #[inline]
            fn try_decrypt_trivial(
                &self,
            ) -> Result<Self::Output, tfhe::shortint::ciphertext::NotTrivialCiphertextError> {
                use crate::utils::rayon::rayon_join4;
                let e0 = &self.$f1;
                let e1 = &self.$f2;
                let e2 = &self.$f3;
                let e3 = &self.$f4;
                let (d0, d1, d2, d3) =
                    rayon_join4(|| e0.try_decrypt_trivial(), || e1.try_decrypt_trivial(), || e2.try_decrypt_trivial(), || e3.try_decrypt_trivial());
                Ok(Self::Output {
                    $f1: d0?,
                    $f2: d1?,
                    $f3: d2?,
                    $f4: d3?,
                })
            }
        }
    };
}
#[allow(unused_imports)]
pub(crate) use drv4_try_trivial_decrypt2;

////////////////////////////////////////////////////////////////////////////////
// DECOMPRESS
////////////////////////////////////////////////////////////////////////////////

impl Decompress for CompressedFheBool
{
    type Output = FheBool;
    #[inline]
    fn decompress(&self) -> Self::Output {
        tfhe::CompressedFheBool::decompress(self)
    }
}

impl<E> Decompress for Vec<E>
where
    E: Decompress + Sync + DefaultInto<<E as Decompress>::Output>,
    <E as Decompress>::Output: Send + Clone
 {
    type Output = Vec<<E as Decompress>::Output>;
    fn decompress(&self) -> Self::Output {
        let def = <E as DefaultInto<<E as Decompress>::Output>>::default_into();
        let mut result = vec![def; self.len()];
        result
            .par_iter_mut()
            .zip(self.par_iter())
            .for_each(|(dst, v)| *dst = v.decompress());
        result
    }
}

impl<E> Decompress for (E,)
where
    E: Decompress,
{
    type Output = (<E as Decompress>::Output,);
    fn decompress(&self) -> Self::Output {
        (self.0.decompress(),)
    }
}

impl<E, const N: usize> Decompress for Box<[E; N]>
where
    E: Decompress + Sync + DefaultInto<<E as Decompress>::Output>,
    <E as Decompress>::Output: Send
 {
    type Output = Box<[<E as Decompress>::Output; N]>;
    fn decompress(&self) -> Self::Output {
        let mut box_result = Box::<[E; N]>::default_into();
        box_result
            .par_iter_mut()
            .zip(self.par_iter())
            .for_each(|(dst, v)| *dst = v.decompress());
        box_result
    }
}

////////////////////////////////////////////////////////////////////////////////
// DECOMPRESS MACROS
////////////////////////////////////////////////////////////////////////////////

#[allow(unused_macros)]
macro_rules! drv_wrapper_decompress {
    ( 
        $TheStruct:ident<$EParam:ident>
    ) => {
        impl<$EParam> Decompress for $TheStruct<$EParam>
        where
            $EParam: Decompress + Sync + DefaultInto<<$EParam as Decompress>::Output>,
            <$EParam as Decompress>::Output: Send + Clone
            {
            type Output = $TheStruct<<$EParam as Decompress>::Output>;
            #[inline]
            fn decompress(&self) -> Self::Output {
                $TheStruct::<<$EParam as Decompress>::Output> (
                    self.0.decompress(),
                )
            }
        }
    };
}
#[allow(unused_imports)]
pub(crate) use drv_wrapper_decompress;

#[allow(unused_macros)]
macro_rules! drv1_decompress {
    ( 
        $TheStruct:ident<$EParam:ident>
        {$f1:ident:$ty1:ty}
    ) => {
        impl<$EParam> Decompress for $TheStruct<$EParam>
        where
            $EParam: Decompress + Sync + DefaultInto<<$EParam as Decompress>::Output>,
            <$EParam as Decompress>::Output: Send + Clone
        {
            type Output = $TheStruct<<$EParam as Decompress>::Output>;
            #[inline]
            fn decompress(&self) -> Self::Output {
                Self::Output {
                    $f1: self.$f1.decompress(),
                }
            }
        }
    };
}
#[allow(unused_imports)]
pub(crate) use drv1_decompress;

#[allow(unused_macros)]
macro_rules! drv1_decompress2 {
    ( 
        $TheStruct:ident<$EParam1:ident, $EParam2:ident>
        {$f1:ident:$ty1:ty}
    ) => {
        impl<$EParam1, $EParam2> Decompress for $TheStruct<$EParam1, $EParam2>
        where
            $EParam1: Decompress + Sync + DefaultInto<<$EParam1 as Decompress>::Output>,
            <$EParam1 as Decompress>::Output: Send + Clone,
            $EParam2: Decompress + Sync + DefaultInto<<$EParam2 as Decompress>::Output>,
            <$EParam2 as Decompress>::Output: Send + Clone
        {
            type Output = $TheStruct<<$EParam1 as Decompress>::Output, <$EParam2 as Decompress>::Output>;
            #[inline]
            fn decompress(&self) -> Self::Output {
                Self::Output {
                    $f1: self.$f1.decompress(),
                }
            }
        }
    };
}
#[allow(unused_imports)]
pub(crate) use drv1_decompress2;

#[allow(unused_macros)]
macro_rules! drv2_decompress {
    ( 
        $TheStruct:ident<$EParam:ident>
        {$f1:ident:$ty1:ty, $f2:ident:$ty2:ty}
    ) => {
        impl<$EParam> Decompress for $TheStruct<$EParam>
        where
            $EParam: Decompress + Sync + DefaultInto<<$EParam as Decompress>::Output>,
            <$EParam as Decompress>::Output: Send + Clone
        {
            type Output = $TheStruct<<$EParam as Decompress>::Output>;
            #[inline]
            fn decompress(&self) -> Self::Output {
                let c1 = &self.$f1;
                let c2 = &self.$f2;
                let (e1, e2) = rayon::join(|| c1.decompress(), || c2.decompress());
                Self::Output {
                    $f1: e1,
                    $f2: e2,
                }
            }
        }
    };
}
#[allow(unused_imports)]
pub(crate) use drv2_decompress;

#[allow(unused_macros)]
macro_rules! drv2_decompress2 {
    ( 
        $TheStruct:ident<$EParam1:ident, $EParam2:ident>
        {$f1:ident:$ty1:ty, $f2:ident:$ty2:ty}
    ) => {
        impl<$EParam1, $EParam2> Decompress for $TheStruct<$EParam1, $EParam2>
        where
            $EParam1: Decompress + Sync + DefaultInto<<$EParam1 as Decompress>::Output>,
            <$EParam1 as Decompress>::Output: Send + Clone,
            $EParam2: Decompress + Sync + DefaultInto<<$EParam2 as Decompress>::Output>,
            <$EParam2 as Decompress>::Output: Send + Clone
        {
            type Output = $TheStruct<<$EParam1 as Decompress>::Output, <$EParam2 as Decompress>::Output>;
            #[inline]
            fn decompress(&self) -> Self::Output {
                let c1 = &self.$f1;
                let c2 = &self.$f2;
                let (e1, e2) = rayon::join(|| c1.decompress(), || c2.decompress());
                Self::Output {
                    $f1: e1,
                    $f2: e2,
                }
            }
        }
    };
}
#[allow(unused_imports)]
pub(crate) use drv2_decompress2;

#[allow(unused_macros)]
macro_rules! drv3_decompress {
    ( 
        $TheStruct:ident<$EParam:ident>
        {$f1:ident:$ty1:ty, $f2:ident:$ty2:ty, $f3:ident:$ty3:ty}
    ) => {
        impl<$EParam> Decompress for $TheStruct<$EParam>
        where
            $EParam: Decompress + Sync + DefaultInto<<$EParam as Decompress>::Output>,
            <$EParam as Decompress>::Output: Send + Clone
        {
            type Output = $TheStruct<<$EParam as Decompress>::Output>;
            #[inline]
            fn decompress(&self) -> Self::Output {
                use crate::utils::rayon::rayon_join3;
                let c1 = &self.$f1;
                let c2 = &self.$f2;
                let c3 = &self.$f3;
                let (e1, e2, e3) = rayon_join3(|| c1.decompress(), || c2.decompress(), || c3.decompress());
                Self::Output {
                    $f1: e1,
                    $f2: e2,
                    $f3: e3,
                }
            }
        }
    };
}
#[allow(unused_imports)]
pub(crate) use drv3_decompress;

#[allow(unused_macros)]
macro_rules! drv3_decompress2 {
    ( 
        $TheStruct:ident<$EParam1:ident, $EParam2:ident>
        {$f1:ident:$ty1:ty, $f2:ident:$ty2:ty, $f3:ident:$ty3:ty}
    ) => {
        impl<$EParam1, $EParam2> Decompress for $TheStruct<$EParam1, $EParam2>
        where
            $EParam1: Decompress + Sync + DefaultInto<<$EParam1 as Decompress>::Output>,
            <$EParam1 as Decompress>::Output: Send + Clone,
            $EParam2: Decompress + Sync + DefaultInto<<$EParam2 as Decompress>::Output>,
            <$EParam2 as Decompress>::Output: Send + Clone
        {
            type Output = $TheStruct<<$EParam1 as Decompress>::Output, <$EParam2 as Decompress>::Output>;
            #[inline]
            fn decompress(&self) -> Self::Output {
                use crate::utils::rayon::rayon_join3;
                let c1 = &self.$f1;
                let c2 = &self.$f2;
                let c3 = &self.$f3;
                let (e1, e2, e3) = rayon_join3(|| c1.decompress(), || c2.decompress(), || c3.decompress());
                Self::Output {
                    $f1: e1,
                    $f2: e2,
                    $f3: e3,
                }
            }
        }
    };
}
#[allow(unused_imports)]
pub(crate) use drv3_decompress2;

#[allow(unused_macros)]
macro_rules! drv4_decompress {
    ( 
        $TheStruct:ident<$EParam:ident>
        {$f1:ident:$ty1:ty, $f2:ident:$ty2:ty, $f3:ident:$ty3:ty, $f4:ident:$ty4:ty}
    ) => {
        impl<$EParam> Decompress for $TheStruct<$EParam>
        where
            $EParam: Decompress + Sync + DefaultInto<<$EParam as Decompress>::Output>,
            <$EParam as Decompress>::Output: Send + Clone
        {
            type Output = $TheStruct<<$EParam as Decompress>::Output>;
            #[inline]
            fn decompress(&self) -> Self::Output {
                use crate::utils::rayon::rayon_join4;
                let c1 = &self.$f1;
                let c2 = &self.$f2;
                let c3 = &self.$f3;
                let c4 = &self.$f4;
                let (e1, e2, e3, e4) = rayon_join4(|| c1.decompress(), || c2.decompress(), || c3.decompress(), || c4.decompress());
                Self::Output {
                    $f1: e1,
                    $f2: e2,
                    $f3: e3,
                    $f4: e4,
                }
            }
        }
    };
}
#[allow(unused_imports)]
pub(crate) use drv4_decompress;

#[allow(unused_macros)]
macro_rules! drv4_decompress2 {
    ( 
        $TheStruct:ident<$EParam1:ident, $EParam2:ident>
        {$f1:ident:$ty1:ty, $f2:ident:$ty2:ty, $f3:ident:$ty3:ty, $f4:ident:$ty4:ty}
    ) => {
        impl<$EParam1, $EParam2> Decompress for $TheStruct<$EParam1, $EParam2>
        where
            $EParam1: Decompress + Sync + DefaultInto<<$EParam1 as Decompress>::Output>,
            <$EParam1 as Decompress>::Output: Send + Clone,
            $EParam2: Decompress + Sync + DefaultInto<<$EParam2 as Decompress>::Output>,
            <$EParam2 as Decompress>::Output: Send + Clone
        {
            type Output = $TheStruct<<$EParam1 as Decompress>::Output, <$EParam2 as Decompress>::Output>;
            #[inline]
            fn decompress(&self) -> Self::Output {
                use crate::utils::rayon::rayon_join4;
                let c1 = &self.$f1;
                let c2 = &self.$f2;
                let c3 = &self.$f3;
                let c4 = &self.$f4;
                let (e1, e2, e3, e4) = rayon_join4(|| c1.decompress(), || c2.decompress(), || c3.decompress(), || c4.decompress());
                Self::Output {
                    $f1: e1,
                    $f2: e2,
                    $f3: e3,
                    $f4: e4,
                }
            }
        }
    };
}
#[allow(unused_imports)]
pub(crate) use drv4_decompress2;

////////////////////////////////////////////////////////////////////////////////
// EXPAND
////////////////////////////////////////////////////////////////////////////////

impl Expand for CompactFheBool
{
    type Output = FheBool;
    #[inline]
    fn expand(&self) -> Self::Output {
        tfhe::CompactFheBool::expand(self)
    }
}

impl<E> Expand for Vec<E>
where
    E: Expand + Sync + DefaultInto<<E as Expand>::Output>,
    <E as Expand>::Output: Send + Clone
 {
    type Output = Vec<<E as Expand>::Output>;
    fn expand(&self) -> Self::Output {
        let def = <E as DefaultInto<<E as Expand>::Output>>::default_into();
        let mut result = vec![def; self.len()];
        result
            .par_iter_mut()
            .zip(self.par_iter())
            .for_each(|(dst, v)| *dst = v.expand());
        result
    }
}

impl<E> Expand for (E,)
where
    E: Expand,
{
    type Output = (<E as Expand>::Output,);
    fn expand(&self) -> Self::Output {
        (self.0.expand(),)
    }
}

impl<E, const N:usize> Expand for Box<[E; N]>
where
    E: Expand + Sync + DefaultInto<<E as Expand>::Output>,
    <E as Expand>::Output: Send
 {
    type Output = Box<[<E as Expand>::Output; N]>;
    fn expand(&self) -> Self::Output {
        let mut box_result = Box::<[E; N]>::default_into();
        box_result
            .par_iter_mut()
            .zip(self.par_iter())
            .for_each(|(dst, v)| *dst = v.expand());
        box_result
    }
}

////////////////////////////////////////////////////////////////////////////////
// EXPAND MACROS
////////////////////////////////////////////////////////////////////////////////

#[allow(unused_macros)]
macro_rules! drv_wrapper_expand {
    ( 
        $TheStruct:ident<$EParam:ident>
    ) => {
        impl<$EParam> Expand for $TheStruct<$EParam>
        where
            $EParam: Expand + Sync + DefaultInto<<$EParam as Expand>::Output>,
            <$EParam as Expand>::Output: Send + Clone
        {
            type Output = $TheStruct<<$EParam as Expand>::Output>;
            #[inline]
            fn expand(&self) -> Self::Output {
                $TheStruct::<<$EParam as Expand>::Output> (
                    self.0.expand(),
                )
            }
        }
    };
}
#[allow(unused_imports)]
pub(crate) use drv_wrapper_expand;

#[allow(unused_macros)]
macro_rules! drv1_expand {
    ( 
        $TheStruct:ident<$EParam:ident>
        {$f1:ident:$ty1:ty}
    ) => {
        impl<$EParam> Expand for $TheStruct<$EParam>
        where
            $EParam: Expand + Sync + DefaultInto<<$EParam as Expand>::Output>,
            <$EParam as Expand>::Output: Send + Clone
        {
            type Output = $TheStruct<<$EParam as Expand>::Output>;
            #[inline]
            fn expand(&self) -> Self::Output {
                Self::Output {
                    $f1: self.$f1.expand()
                }
            }
        }
    };
}
#[allow(unused_imports)]
pub(crate) use drv1_expand;

#[allow(unused_macros)]
macro_rules! drv1_expand2 {
    ( 
        $TheStruct:ident<$EParam1:ident, $EParam2:ident>
        {$f1:ident:$ty1:ty}
    ) => {
        impl<$EParam1, $EParam2> Expand for $TheStruct<$EParam1, $EParam2>
        where
            $EParam1: Expand + Sync + DefaultInto<<$EParam1 as Expand>::Output>,
            <$EParam1 as Expand>::Output: Send + Clone,
            $EParam2: Expand + Sync + DefaultInto<<$EParam2 as Expand>::Output>,
            <$EParam2 as Expand>::Output: Send + Clone
{
            type Output = $TheStruct<<$EParam1 as Expand>::Output, <$EParam2 as Expand>::Output>;
            #[inline]
            fn expand(&self) -> Self::Output {
                Self::Output {
                    $f1: self.$f1.expand()
                }
            }
        }
    };
}
#[allow(unused_imports)]
pub(crate) use drv1_expand2;

#[allow(unused_macros)]
macro_rules! drv2_expand {
    ( 
        $TheStruct:ident<$EParam:ident>
        {$f1:ident:$ty1:ty, $f2:ident:$ty2:ty}
    ) => {
        impl<$EParam> Expand for $TheStruct<$EParam>
        where
            $EParam: Expand + Sync + DefaultInto<<$EParam as Expand>::Output>,
            <$EParam as Expand>::Output: Send + Clone
        {
            type Output = $TheStruct<<$EParam as Expand>::Output>;
            #[inline]
            fn expand(&self) -> Self::Output {
                let c1 = &self.$f1;
                let c2 = &self.$f2;
                let (e1, e2) = rayon::join(|| c1.expand(), || c2.expand());
                Self::Output {
                    $f1: e1,
                    $f2: e2,
                }
            }
        }
    };
}
#[allow(unused_imports)]
pub(crate) use drv2_expand;

#[allow(unused_macros)]
macro_rules! drv2_expand2 {
    ( 
        $TheStruct:ident<$EParam1:ident, $EParam2:ident>
        {$f1:ident:$ty1:ty, $f2:ident:$ty2:ty}
    ) => {
        impl<$EParam1, $EParam2> Expand for $TheStruct<$EParam1, $EParam2>
        where
            $EParam1: Expand + Sync + DefaultInto<<$EParam1 as Expand>::Output>,
            <$EParam1 as Expand>::Output: Send + Clone,
            $EParam2: Expand + Sync + DefaultInto<<$EParam2 as Expand>::Output>,
            <$EParam2 as Expand>::Output: Send + Clone
{
            type Output = $TheStruct<<$EParam1 as Expand>::Output, <$EParam2 as Expand>::Output>;
            #[inline]
            fn expand(&self) -> Self::Output {
                let c1 = &self.$f1;
                let c2 = &self.$f2;
                let (e1, e2) = rayon::join(|| c1.expand(), || c2.expand());
                Self::Output {
                    $f1: e1,
                    $f2: e2,
                }
            }
        }
    };
}
#[allow(unused_imports)]
pub(crate) use drv2_expand2;

#[allow(unused_macros)]
macro_rules! drv3_expand {
    ( 
        $TheStruct:ident<$EParam:ident>
        {$f1:ident:$ty1:ty, $f2:ident:$ty2:ty, $f3:ident:$ty3:ty}
    ) => {
        impl<$EParam> Expand for $TheStruct<$EParam>
        where
            $EParam: Expand + Sync + DefaultInto<<$EParam as Expand>::Output>,
            <$EParam as Expand>::Output: Send + Clone
        {
            type Output = $TheStruct<<$EParam as Expand>::Output>;
            #[inline]
            fn expand(&self) -> Self::Output {
                use crate::utils::rayon::rayon_join3;
                let c1 = &self.$f1;
                let c2 = &self.$f2;
                let c3 = &self.$f3;
                let (e1, e2, e3) = rayon_join3(|| c1.expand(), || c2.expand(), || c3.expand());
                Self::Output {
                    $f1: e1,
                    $f2: e2,
                    $f3: e3,
                }
            }
        }
    };
}
#[allow(unused_imports)]
pub(crate) use drv3_expand;

#[allow(unused_macros)]
macro_rules! drv3_expand2 {
    ( 
        $TheStruct:ident<$EParam1:ident, $EParam2:ident>
        {$f1:ident:$ty1:ty, $f2:ident:$ty2:ty, $f3:ident:$ty3:ty}
    ) => {
        impl<$EParam1, $EParam2> Expand for $TheStruct<$EParam1, $EParam2>
        where
            $EParam1: Expand + Sync + DefaultInto<<$EParam1 as Expand>::Output>,
            <$EParam1 as Expand>::Output: Send + Clone,
            $EParam2: Expand + Sync + DefaultInto<<$EParam2 as Expand>::Output>,
            <$EParam2 as Expand>::Output: Send + Clone
{
            type Output = $TheStruct<<$EParam1 as Expand>::Output, <$EParam2 as Expand>::Output>;
            #[inline]
            fn expand(&self) -> Self::Output {
                use crate::utils::rayon::rayon_join3;
                let c1 = &self.$f1;
                let c2 = &self.$f2;
                let c3 = &self.$f3;
                let (e1, e2, e3) = rayon_join3(|| c1.expand(), || c2.expand(), || c3.expand());
                Self::Output {
                    $f1: e1,
                    $f2: e2,
                    $f3: e3,
                }
            }
        }
    };
}
#[allow(unused_imports)]
pub(crate) use drv3_expand2;

#[allow(unused_macros)]
macro_rules! drv4_expand {
    ( 
        $TheStruct:ident<$EParam:ident>
        {$f1:ident:$ty1:ty, $f2:ident:$ty2:ty, $f3:ident:$ty3:ty, $f4:ident:$ty4:ty}
    ) => {
        impl<$EParam> Expand for $TheStruct<$EParam>
        where
            $EParam: Expand + Sync + DefaultInto<<$EParam as Expand>::Output>,
            <$EParam as Expand>::Output: Send + Clone
        {
            type Output = $TheStruct<<$EParam as Expand>::Output>;
            #[inline]
            fn expand(&self) -> Self::Output {
                use crate::utils::rayon::rayon_join4;
                let c1 = &self.$f1;
                let c2 = &self.$f2;
                let c3 = &self.$f3;
                let c4 = &self.$f4;
                let (e1, e2, e3, e4) = rayon_join4(|| c1.expand(), || c2.expand(), || c3.expand(), || c4.expand());
                Self::Output {
                    $f1: e1,
                    $f2: e2,
                    $f3: e3,
                    $f4: e4,
                }
            }
        }
    };
}
#[allow(unused_imports)]
pub(crate) use drv4_expand;

#[allow(unused_macros)]
macro_rules! drv4_expand2 {
    ( 
        $TheStruct:ident<$EParam1:ident, $EParam2:ident>
        {$f1:ident:$ty1:ty, $f2:ident:$ty2:ty, $f3:ident:$ty3:ty, $f4:ident:$ty4:ty}
    ) => {
        impl<$EParam1, $EParam2> Expand for $TheStruct<$EParam1, $EParam2>
        where
            $EParam1: Expand + Sync + DefaultInto<<$EParam1 as Expand>::Output>,
            <$EParam1 as Expand>::Output: Send + Clone,
            $EParam2: Expand + Sync + DefaultInto<<$EParam2 as Expand>::Output>,
            <$EParam2 as Expand>::Output: Send + Clone
{
            type Output = $TheStruct<<$EParam1 as Expand>::Output, <$EParam2 as Expand>::Output>;
            #[inline]
            fn expand(&self) -> Self::Output {
                use crate::utils::rayon::rayon_join4;
                let c1 = &self.$f1;
                let c2 = &self.$f2;
                let c3 = &self.$f3;
                let c4 = &self.$f4;
                let (e1, e2, e3, e4) = rayon_join4(|| c1.expand(), || c2.expand(), || c3.expand(), || c4.expand());
                Self::Output {
                    $f1: e1,
                    $f2: e2,
                    $f3: e3,
                    $f4: e4,
                }
            }
        }
    };
}
#[allow(unused_imports)]
pub(crate) use drv4_expand2;

#[allow(unused_macros)]
macro_rules! derive_wrapper_encrypt_decrypt {
    ( 
        $TheStruct:ident<$EParam:ident>
        ($ty1:ty)
    ) => {
        drv_wrapper_default_into! { $TheStruct<$EParam>($ty1) }
        drv_wrapper_default_into_with_key! { $TheStruct<$EParam>($ty1) }
        drv_wrapper_encrypt! { $TheStruct<$EParam> }
        drv_wrapper_decrypt! { $TheStruct<$EParam> }
        drv_wrapper_decompress! { $TheStruct<$EParam> }
        drv_wrapper_expand! { $TheStruct<$EParam> }
        drv_wrapper_trivial_encrypt! { $TheStruct<$EParam> }
        drv_wrapper_try_trivial_decrypt! { $TheStruct<$EParam> }
    };
}
#[allow(unused_imports)]
pub(crate) use derive_wrapper_encrypt_decrypt;

#[allow(unused_macros)]
macro_rules! derive1_encrypt_decrypt {
    ( 
        $TheStruct:ident<$EParam:ident>
        {$f1:ident:$ty1:ty}
    ) => {
        drv_default_into! { $TheStruct<$EParam> {$f1:$ty1,} }
        drv_default_into_with_key! { $TheStruct<$EParam> {$f1:$ty1,} }
        drv1_encrypt! { $TheStruct<$EParam> {$f1:$ty1} }
        drv1_decrypt! { $TheStruct<$EParam> {$f1:$ty1} }
        drv1_decompress! { $TheStruct<$EParam> {$f1:$ty1} }
        drv1_expand! { $TheStruct<$EParam> {$f1:$ty1} }
        drv1_trivial_encrypt! { $TheStruct<$EParam> {$f1:$ty1} }
        drv1_try_trivial_decrypt! { $TheStruct<$EParam> {$f1:$ty1} }
    };
}
#[allow(unused_imports)]
pub(crate) use derive1_encrypt_decrypt;

#[allow(unused_macros)]
macro_rules! derive1_encrypt_decrypt2 {
    ( 
        $TheStruct:ident<$EParam1:ident, $EParam2:ident>
        {$f1:ident:$ty1:ty}
    ) => {
        drv_default_into2! { $TheStruct<$EParam1, $EParam2> {$f1:$ty1,} }
        drv_default_into_with_key2! { $TheStruct<$EParam1, $EParam2> {$f1:$ty1,} }
        drv1_encrypt2! { $TheStruct<$EParam1, $EParam2> {$f1:$ty1} }
        drv1_decrypt2! { $TheStruct<$EParam1, $EParam2> {$f1:$ty1} }
        drv1_decompress2! { $TheStruct<$EParam1, $EParam2> {$f1:$ty1} }
        drv1_expand2! { $TheStruct<$EParam1, $EParam2> {$f1:$ty1} }
        drv1_trivial_encrypt2! { $TheStruct<$EParam1, $EParam2> {$f1:$ty1} }
        drv1_try_trivial_decrypt2! { $TheStruct<$EParam1, $EParam2> {$f1:$ty1} }
    };
}
#[allow(unused_imports)]
pub(crate) use derive1_encrypt_decrypt2;

#[allow(unused_macros)]
macro_rules! derive2_encrypt_decrypt {
    ( 
        $TheStruct:ident<$EParam:ident>
        {$f1:ident:$ty1:ty, $f2:ident:$ty2:ty}
    ) => {
        drv_default_into! { $TheStruct<$EParam> {$f1:$ty1,$f2:$ty2,} }
        drv_default_into_with_key! { $TheStruct<$EParam> {$f1:$ty1,$f2:$ty2,} }
        drv2_encrypt! { $TheStruct<$EParam> {$f1:$ty1,$f2:$ty2} }
        drv2_decrypt! { $TheStruct<$EParam> {$f1:$ty1,$f2:$ty2} }
        drv2_decompress! { $TheStruct<$EParam> {$f1:$ty1,$f2:$ty2} }
        drv2_expand! { $TheStruct<$EParam> {$f1:$ty1,$f2:$ty2} }
        drv2_trivial_encrypt! { $TheStruct<$EParam> {$f1:$ty1,$f2:$ty2} }
        drv2_try_trivial_decrypt! { $TheStruct<$EParam> {$f1:$ty1,$f2:$ty2} }
    };
}
#[allow(unused_imports)]
pub(crate) use derive2_encrypt_decrypt;

#[allow(unused_macros)]
macro_rules! derive2_encrypt_decrypt2 {
    ( 
        $TheStruct:ident<$EParam1:ident, $EParam2:ident>
        {$f1:ident:$ty1:ty, $f2:ident:$ty2:ty}
    ) => {
        drv_default_into2! { $TheStruct<$EParam1, $EParam2> {$f1:$ty1,$f2:$ty2,} }
        drv_default_into_with_key2! { $TheStruct<$EParam1, $EParam2> {$f1:$ty1,$f2:$ty2,} }
        drv2_encrypt2! { $TheStruct<$EParam1, $EParam2> {$f1:$ty1,$f2:$ty2} }
        drv2_decrypt2! { $TheStruct<$EParam1, $EParam2> {$f1:$ty1,$f2:$ty2} }
        drv2_decompress2! { $TheStruct<$EParam1, $EParam2> {$f1:$ty1,$f2:$ty2} }
        drv2_expand2! { $TheStruct<$EParam1, $EParam2> {$f1:$ty1,$f2:$ty2} }
        drv2_trivial_encrypt2! { $TheStruct<$EParam1, $EParam2> {$f1:$ty1,$f2:$ty2} }
        drv2_try_trivial_decrypt2! { $TheStruct<$EParam1, $EParam2> {$f1:$ty1,$f2:$ty2} }
    };
}
#[allow(unused_imports)]
pub(crate) use derive2_encrypt_decrypt2;

#[allow(unused_macros)]
macro_rules! derive3_encrypt_decrypt {
    ( 
        $TheStruct:ident<$EParam:ident>
        {$f1:ident:$ty1:ty, $f2:ident:$ty2:ty, $f3:ident:$ty3:ty}
    ) => {
        drv_default_into! { $TheStruct<$EParam> {$f1:$ty1,$f2:$ty2,$f3:$ty3,} }
        drv_default_into_with_key! { $TheStruct<$EParam> {$f1:$ty1,$f2:$ty2,$f3:$ty3,} }
        drv3_encrypt! { $TheStruct<$EParam> {$f1:$ty1,$f2:$ty2,$f3:$ty3} }
        drv3_decrypt! { $TheStruct<$EParam> {$f1:$ty1,$f2:$ty2,$f3:$ty3} }
        drv3_decompress! { $TheStruct<$EParam> {$f1:$ty1,$f2:$ty2,$f3:$ty3} }
        drv3_expand! { $TheStruct<$EParam> {$f1:$ty1,$f2:$ty2,$f3:$ty3} }
        drv3_trivial_encrypt! { $TheStruct<$EParam> {$f1:$ty1,$f2:$ty2,$f3:$ty3} }
        drv3_try_trivial_decrypt! { $TheStruct<$EParam> {$f1:$ty1,$f2:$ty2,$f3:$ty3} }
    };
}
#[allow(unused_imports)]
pub(crate) use derive3_encrypt_decrypt;

#[allow(unused_macros)]
macro_rules! derive3_encrypt_decrypt2 {
    ( 
        $TheStruct:ident<$EParam1:ident, $EParam2:ident>
        {$f1:ident:$ty1:ty, $f2:ident:$ty2:ty, $f3:ident:$ty3:ty}
    ) => {
        drv_default_into2! { $TheStruct<$EParam1, $EParam2> {$f1:$ty1,$f2:$ty2,$f3:$ty3,} }
        drv_default_into_with_key2! { $TheStruct<$EParam1, $EParam2> {$f1:$ty1,$f2:$ty2,$f3:$ty3,} }
        drv3_encrypt2! { $TheStruct<$EParam1, $EParam2> {$f1:$ty1,$f2:$ty2,$f3:$ty3} }
        drv3_decrypt2! { $TheStruct<$EParam1, $EParam2> {$f1:$ty1,$f2:$ty2,$f3:$ty3} }
        drv3_decompress2! { $TheStruct<$EParam1, $EParam2> {$f1:$ty1,$f2:$ty2,$f3:$ty3} }
        drv3_expand2! { $TheStruct<$EParam1, $EParam2> {$f1:$ty1,$f2:$ty2,$f3:$ty3} }
        drv3_trivial_encrypt2! { $TheStruct<$EParam1, $EParam2> {$f1:$ty1,$f2:$ty2,$f3:$ty3} }
        drv3_try_trivial_decrypt2! { $TheStruct<$EParam1, $EParam2> {$f1:$ty1,$f2:$ty2,$f3:$ty3} }
    };
}
#[allow(unused_imports)]
pub(crate) use derive3_encrypt_decrypt2;

#[allow(unused_macros)]
macro_rules! derive4_encrypt_decrypt {
    ( 
        $TheStruct:ident<$EParam:ident>
        {$f1:ident:$ty1:ty, $f2:ident:$ty2:ty, $f3:ident:$ty3:ty, $f4:ident:$ty4:ty}
    ) => {
        drv_default_into! { $TheStruct<$EParam> {$f1:$ty1,$f2:$ty2,$f3:$ty3,$f4:$ty4,} }
        drv_default_into_with_key! { $TheStruct<$EParam> {$f1:$ty1,$f2:$ty2,$f3:$ty3,$f4:$ty4,} }
        drv4_encrypt! { $TheStruct<$EParam> {$f1:$ty1,$f2:$ty2,$f3:$ty3,$f4:$ty4} }
        drv4_decrypt! { $TheStruct<$EParam> {$f1:$ty1,$f2:$ty2,$f3:$ty3,$f4:$ty4} }
        drv4_decompress! { $TheStruct<$EParam> {$f1:$ty1,$f2:$ty2,$f3:$ty3,$f4:$ty4} }
        drv4_expand! { $TheStruct<$EParam> {$f1:$ty1,$f2:$ty2,$f3:$ty3,$f4:$ty4} }
        drv4_trivial_encrypt! { $TheStruct<$EParam> {$f1:$ty1,$f2:$ty2,$f3:$ty3,$f4:$ty4} }
        drv4_try_trivial_decrypt! { $TheStruct<$EParam> {$f1:$ty1,$f2:$ty2,$f3:$ty3,$f4:$ty4} }
    };
}
#[allow(unused_imports)]
pub(crate) use derive4_encrypt_decrypt;

#[allow(unused_macros)]
macro_rules! derive4_encrypt_decrypt2 {
    ( 
        $TheStruct:ident<$EParam1:ident, $EParam2:ident>
        {$f1:ident:$ty1:ty, $f2:ident:$ty2:ty, $f3:ident:$ty3:ty, $f4:ident:$ty4:ty}
    ) => {
        drv_default_into2! { $TheStruct<$EParam1, $EParam2> {$f1:$ty1,$f2:$ty2,$f3:$ty3,$f4:$ty4,} }
        drv_default_into_with_key2! { $TheStruct<$EParam1, $EParam2> {$f1:$ty1,$f2:$ty2,$f3:$ty3,$f4:$ty4,} }
        drv4_encrypt2! { $TheStruct<$EParam1, $EParam2> {$f1:$ty1,$f2:$ty2,$f3:$ty3,$f4:$ty4} }
        drv4_decrypt2! { $TheStruct<$EParam1, $EParam2> {$f1:$ty1,$f2:$ty2,$f3:$ty3,$f4:$ty4} }
        drv4_decompress2! { $TheStruct<$EParam1, $EParam2> {$f1:$ty1,$f2:$ty2,$f3:$ty3,$f4:$ty4} }
        drv4_expand2! { $TheStruct<$EParam1, $EParam2> {$f1:$ty1,$f2:$ty2,$f3:$ty3,$f4:$ty4} }
        drv4_trivial_encrypt2! { $TheStruct<$EParam1, $EParam2> {$f1:$ty1,$f2:$ty2,$f3:$ty3,$f4:$ty4} }
        drv4_try_trivial_decrypt2! { $TheStruct<$EParam1, $EParam2> {$f1:$ty1,$f2:$ty2,$f3:$ty3,$f4:$ty4} }
    };
}
#[allow(unused_imports)]
pub(crate) use derive4_encrypt_decrypt2;

#[allow(unused_macros)]
macro_rules! derive4_only_decrypt {
    ( 
        $TheStruct:ident<$EParam:ident>
        {$f1:ident:$ty1:ty, $f2:ident:$ty2:ty, $f3:ident:$ty3:ty, $f4:ident:$ty4:ty}
    ) => {
        drv_default_into! { $TheStruct<$EParam> {$f1:$ty1,$f2:$ty2,$f3:$ty3,$f4:$ty4,} }
        drv_default_into_with_key! { $TheStruct<$EParam> {$f1:$ty1,$f2:$ty2,$f3:$ty3,$f4:$ty4,} }
        drv_no_encrypt! { $TheStruct<$EParam> }
        drv4_decrypt! { $TheStruct<$EParam> {$f1:$ty1,$f2:$ty2,$f3:$ty3,$f4:$ty4} }
        drv4_decompress! { $TheStruct<$EParam> {$f1:$ty1,$f2:$ty2,$f3:$ty3,$f4:$ty4} }
        drv4_expand! { $TheStruct<$EParam> {$f1:$ty1,$f2:$ty2,$f3:$ty3,$f4:$ty4} }
        drv_no_trivial_encrypt! { $TheStruct<$EParam> }
        drv4_try_trivial_decrypt! { $TheStruct<$EParam> {$f1:$ty1,$f2:$ty2,$f3:$ty3,$f4:$ty4} }
    };
}
#[allow(unused_imports)]
pub(crate) use derive4_only_decrypt;

////////////////////////////////////////////////////////////////////////////////
// Tests
////////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod test {
    use tfhe::set_server_key;

    use crate::default_into::{drv_default_into, drv_default_into_with_key, drv_wrapper_default_into, drv_wrapper_default_into_with_key};
    use crate::test_util::try_load_or_gen_test_keys;
    use crate::test_util::broadcast_set_server_key;
    use super::*;

    #[derive(Clone, PartialEq, Debug)]    
    struct AStruct<T> {
        a: BStruct<T>,
        b: T,
        c: Vec<T>,
        d: Vec<BStruct<T>>,
    }

    #[derive(Clone, PartialEq, Debug)]    
    struct BStruct<T> {
        e: T,
        f: T,
    }

    #[derive(Clone, PartialEq, Debug)]    
    struct CStruct<T> {
        g: T,
        h: Vec<T>,
    }

    #[derive(Clone, PartialEq, Debug)]    
    struct EStruct<T> {
        g: T,
        h: Vec<T>,
        k: Vec<T>,
    }

    #[derive(Clone, PartialEq, Debug)]    
    struct FStruct<T> {
        e: T,
    }

    #[derive(Clone, PartialEq, Debug)]    
    struct DStruct<T>(AStruct<T>);
    derive_wrapper_encrypt_decrypt!{ DStruct<T> (AStruct<T>) }

    drv_default_into! { BStruct<T> {e:T, f:T,} }
    drv_default_into_with_key! { BStruct<T> {e:T, f:T,} }
    drv2_encrypt! { BStruct<T> {e:T, f:T} }
    drv2_decrypt! { BStruct<T> {e:T, f:T} }
    drv2_decompress! { BStruct<T> {e:T, f:T} }
    drv2_expand! { BStruct<T> {e:T, f:T} }
    drv2_trivial_encrypt! { BStruct<T> {e:T, f:T} }
    drv2_try_trivial_decrypt! { BStruct<T> {e:T, f:T} }

    drv_default_into! { EStruct<T> {g:T, h:Vec<T>, k:Vec<T>,} }
    drv_default_into_with_key! { EStruct<T> {g:T, h:Vec<T>, k:Vec<T>,} }
    drv3_encrypt! { EStruct<T> {g:T, h:Vec<T>, k:Vec<T>} }
    drv3_decrypt! { EStruct<T> {g:T, h:Vec<T>, k:Vec<T>} }
    drv3_decompress! { EStruct<T> {g:T, h:Vec<T>, k:Vec<T>} }
    drv3_expand! { EStruct<T> {g:T, h:Vec<T>, k:Vec<T>} }
    drv3_trivial_encrypt! { EStruct<T> {g:T, h:Vec<T>, k:Vec<T>} }
    drv3_try_trivial_decrypt! { EStruct<T> {g:T, h:Vec<T>, k:Vec<T>} }

    drv_default_into! { AStruct<T> {a:BStruct<T>, b:T, c:Vec<T>, d:Vec<BStruct<T>>,} }
    drv_default_into_with_key! { AStruct<T> {a:BStruct<T>, b:T, c:Vec<T>, d:Vec<BStruct<T>>,} }
    drv4_encrypt! { AStruct<T> {a:BStruct<T>, b:T, c:Vec<T>, d:Vec<BStruct<T>>} }
    drv4_decrypt! { AStruct<T> {a:BStruct<T>, b:T, c:Vec<T>, d:Vec<BStruct<T>>} }
    drv4_decompress! { AStruct<T> {a:BStruct<T>, b:T, c:Vec<T>, d:Vec<BStruct<T>>} }
    drv4_expand! { AStruct<T> {a:BStruct<T>, b:T, c:Vec<T>, d:Vec<BStruct<T>>} }
    drv4_trivial_encrypt! { AStruct<T> {a:BStruct<T>, b:T, c:Vec<T>, d:Vec<BStruct<T>>} }
    drv4_try_trivial_decrypt! { AStruct<T> {a:BStruct<T>, b:T, c:Vec<T>, d:Vec<BStruct<T>>} }

    drv_default_into! { CStruct<T> {g:T, h:Vec<T>,} }
    drv_default_into_with_key! { CStruct<T> {g:T, h:Vec<T>,} }
    drv2_decrypt! { CStruct<T> {g:T, h:Vec<T>} }
    drv2_decompress! { CStruct<T> {g:T, h:Vec<T>} }
    drv_no_encrypt! { CStruct<T> }
    drv_no_trivial_encrypt! { CStruct<T> }

    drv_default_into! { FStruct<T> {e:T,} }
    drv_default_into_with_key! { FStruct<T> {e:T,} }
    drv1_encrypt! { FStruct<T> {e:T} }
    drv1_decrypt! { FStruct<T> {e:T} }
    drv1_decompress! { FStruct<T> {e:T} }
    drv1_expand! { FStruct<T> {e:T} }
    drv1_trivial_encrypt! { FStruct<T> {e:T} }
    drv1_try_trivial_decrypt! { FStruct<T> {e:T} }

    fn tfhe_decrypt(b: &FheBool, ck: &ClientKey) -> bool {
        tfhe::prelude::FheDecrypt::<bool>::decrypt(b, ck)
    }

    #[test]
    fn test_encrypt() {
        let (ck, sk) = try_load_or_gen_test_keys(false);

        broadcast_set_server_key(&sk);
        set_server_key(sk);
        //let compact_public_key = CompactPublicKey::try_new(&ck).unwrap();

        let enc_b: FheBool = bool::default_into();
        let clear_b = tfhe_decrypt(&enc_b, &ck);
        assert!(!clear_b);

        let enc_b: FheBool = bool::default_into();
        let clear_b = enc_b.try_decrypt_trivial().unwrap();
        assert!(!clear_b);

        let clear_b: bool = FheBool::default_into();
        assert!(!clear_b);

        let enc_s = BStruct::<bool>::default_into();
        assert!(!tfhe_decrypt(&enc_s.e, &ck));
        assert!(!tfhe_decrypt(&enc_s.f, &ck));
        
        let enc_s = AStruct::<bool>::default_into();
        assert!(!tfhe_decrypt(&enc_s.a.e, &ck));
        assert!(!tfhe_decrypt(&enc_s.a.f, &ck));
        assert!(!tfhe_decrypt(&enc_s.b, &ck));
        assert_eq!(enc_s.c.len(), 0);
        assert_eq!(enc_s.d.len(), 0);

        let b = FStruct::<bool> {
            e: true,
        };
        let enc_b = FStruct::<FheBool>::encrypt_ref(&b, &ck);
        assert!(tfhe_decrypt(&enc_b.e, &ck));
        let clear_b = enc_b.decrypt(&ck);
        assert_eq!(clear_b, b);

        let enc_b = FStruct::<FheBool>::encrypt_trivial_ref(&b);
        assert!(tfhe_decrypt(&enc_b.e, &ck));
        let clear_b = enc_b.try_decrypt_trivial().unwrap();
        assert_eq!(clear_b, b);

        let cmp_b = FStruct::<CompressedFheBool>::encrypt_ref(&b, &ck);
        let enc_b = cmp_b.decompress();
        assert!(tfhe_decrypt(&enc_b.e, &ck));
        let clear_b = enc_b.decrypt(&ck);
        assert_eq!(clear_b, b);

        // let cmp_b = FStruct::<CompactFheBool>::encrypt_ref(&b, &compact_public_key);
        // let enc_b = cmp_b.expand();
        // assert_eq!(tfhe_decrypt(&enc_b.e, &ck), true);
        // let clear_b = enc_b.decrypt(&ck);
        // assert_eq!(clear_b, b);

        let b = BStruct::<bool> {
            e: true,
            f: false
        };
        let enc_b = BStruct::<FheBool>::encrypt_ref(&b, &ck);
        assert!(tfhe_decrypt(&enc_b.e, &ck));
        assert!(!tfhe_decrypt(&enc_b.f, &ck));
        let clear_b = enc_b.decrypt(&ck);
        assert_eq!(clear_b, b);

        let enc_b = BStruct::<FheBool>::encrypt_trivial_ref(&b);
        assert!(tfhe_decrypt(&enc_b.e, &ck));
        assert!(!tfhe_decrypt(&enc_b.f, &ck));
        let clear_b = enc_b.try_decrypt_trivial().unwrap();
        assert_eq!(clear_b, b);

        let cmp_b = BStruct::<CompressedFheBool>::encrypt_ref(&b, &ck);
        let enc_b = cmp_b.decompress();
        assert!(tfhe_decrypt(&enc_b.e, &ck));
        assert!(!tfhe_decrypt(&enc_b.f, &ck));
        let clear_b = enc_b.decrypt(&ck);
        assert_eq!(clear_b, b);

        // let cmp_b = BStruct::<CompactFheBool>::encrypt_ref(&b, &compact_public_key);
        // let enc_b = cmp_b.expand();
        // assert_eq!(tfhe_decrypt(&enc_b.e, &ck), true);
        // assert_eq!(tfhe_decrypt(&enc_b.f, &ck), false);
        // let clear_b = enc_b.decrypt(&ck);
        // assert_eq!(clear_b, b);

        let b = AStruct::<bool> {
            a: BStruct::<bool> { e:true, f:true },
            b: true,
            c: vec![true; 3],
            d: vec![BStruct::<bool> { e:true, f:true }; 2]
        };
        let enc_b = AStruct::<FheBool>::encrypt_ref(&b, &ck);
        assert!(tfhe_decrypt(&enc_b.a.e, &ck));
        assert!(tfhe_decrypt(&enc_b.a.f, &ck));
        assert!(tfhe_decrypt(&enc_b.b, &ck));
        assert!(tfhe_decrypt(&enc_b.c[0], &ck));
        assert!(tfhe_decrypt(&enc_b.c[1], &ck));
        assert!(tfhe_decrypt(&enc_b.c[2], &ck));
        assert_eq!(enc_b.c.len(), 3);
        assert_eq!(enc_b.d.len(), 2);
        assert!(tfhe_decrypt(&enc_b.d[0].e, &ck));
        assert!(tfhe_decrypt(&enc_b.d[0].f, &ck));
        assert!(tfhe_decrypt(&enc_b.d[1].e, &ck));
        assert!(tfhe_decrypt(&enc_b.d[1].f, &ck));
        let clear_b = enc_b.decrypt(&ck);
        assert_eq!(clear_b, b);

        let enc_b = AStruct::<FheBool>::encrypt_trivial_ref(&b);
        let clear_b = enc_b.try_decrypt_trivial().unwrap();
        assert_eq!(clear_b, b);

        let cmp_b = AStruct::<CompressedFheBool>::encrypt_ref(&b, &ck);
        let enc_b = cmp_b.decompress();
        let clear_b = enc_b.decrypt(&ck);
        assert_eq!(clear_b, b);

        // let cmp_b = AStruct::<CompactFheBool>::encrypt_ref(&b, &compact_public_key);
        // let enc_b = cmp_b.expand();
        // let clear_b = enc_b.decrypt(&ck);
        // assert_eq!(clear_b, b);

        let d = DStruct::<bool>(b.clone());
        let enc_d = DStruct::<FheBool>::encrypt_ref(&d, &ck);
        let clear_d = enc_d.decrypt(&ck);
        assert_eq!(clear_d, d);

        let enc_d = DStruct::<FheBool>::encrypt_trivial_ref(&d);
        let clear_d = enc_d.try_decrypt_trivial().unwrap();
        assert_eq!(clear_d, d);

        let cmp_d = DStruct::<CompressedFheBool>::encrypt_ref(&d, &ck);
        let enc_d = cmp_d.decompress();
        let clear_d = enc_d.decrypt(&ck);
        assert_eq!(clear_d, d);

        let default_c = CStruct::<bool> {
            g: false,
            h: vec![]
        };

        let b = CStruct::<bool> {
            g: true,
            h: vec![false;25]
        };

        let cmp_b = CStruct::<CompressedFheBool>::encrypt_ref(&b, &ck);
        let enc_b = cmp_b.decompress();
        let clear_b = enc_b.decrypt(&ck);
        assert_eq!(clear_b, default_c);

        let enc_b = CStruct::<FheBool>::encrypt_trivial_ref(&b);
        let clear_b = enc_b.decrypt(&ck);
        assert_eq!(clear_b, default_c);
    }
}
