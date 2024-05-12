use tfhe::shortint::ciphertext::NotTrivialCiphertextError;

pub trait EncryptRef<C, Key> {
    fn encrypt_ref(value: &C, key: &Key) -> Self;
}

pub trait EncryptInto<E, Key>
{
    fn encrypt_into(&self, key: &Key) -> E;
}

impl<C, E, Key> EncryptInto<E, Key> for C
where
    E: EncryptRef<C, Key>,
{
    #[inline]
    fn encrypt_into(&self, key: &Key) -> E {
        E::encrypt_ref(self, key)
    }
}

pub trait Decrypt<Key> {
    type Output;
    fn decrypt(&self, ck: &Key) -> Self::Output;
}

pub trait TrivialEncryptRef<T> {
    fn encrypt_trivial_ref(value: &T) -> Self;
}

pub trait TrivialEncryptInto<E>
{
    fn encrypt_trivial_into(&self) -> E;
}

impl<C, E> TrivialEncryptInto<E> for C
where
    E: TrivialEncryptRef<C>,
{
    #[inline]
    fn encrypt_trivial_into(&self) -> E {
        E::encrypt_trivial_ref(self)
    }
}

pub trait TryTrivialDecrypt {
    type Output;
    fn try_decrypt_trivial(&self) -> Result<Self::Output, NotTrivialCiphertextError>;
}

pub trait Decompress {
    type Output;
    fn decompress(&self) -> Self::Output;
}

pub trait Expand {
    type Output;
    fn expand(&self) -> Self::Output;
}
