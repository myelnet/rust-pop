use crate::index::{Error, HashedKey};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256 as Sha256Hasher};
use std::borrow::Borrow;
use std::cmp::Ordering;
use std::hash::Hasher;
use std::ops::Deref;
use std::{mem, slice};

/// Algorithm used as the hasher for the Hamt.
pub trait HashAlgorithm {
    fn hash<X: ?Sized>(key: &X) -> HashedKey
    where
        X: Hash;
}

/// Type is needed because the Sha256 hasher does not implement `std::hash::Hasher`
#[derive(Default)]
struct Sha2HasherWrapper(Sha256Hasher);

impl Hasher for Sha2HasherWrapper {
    fn finish(&self) -> u64 {
        // u64 hash not used in hamt
        0
    }

    fn write(&mut self, bytes: &[u8]) {
        self.0.update(bytes);
    }
}

/// Sha256 hashing algorithm used for hashing keys in the Hamt.
#[derive(Debug)]
pub enum Sha256 {}

impl HashAlgorithm for Sha256 {
    fn hash<X: ?Sized>(key: &X) -> HashedKey
    where
        X: Hash,
    {
        let mut hasher = Sha2HasherWrapper::default();
        key.hash(&mut hasher);
        hasher.0.finalize().into()
    }
}

/// Key type to be used to serialize as byte string instead of a `u8` array.
/// This type is used as a default for the `Hamt` as this is the only allowed type
/// with the go implementation.
#[derive(Eq, PartialOrd, Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(transparent)]
pub struct BytesKey(#[serde(with = "serde_bytes")] pub Vec<u8>);

impl Hash for BytesKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        state.write(&self.0);
    }
}

impl Borrow<[u8]> for BytesKey {
    fn borrow(&self) -> &[u8] {
        &self.0
    }
}

impl Borrow<Vec<u8>> for BytesKey {
    fn borrow(&self) -> &Vec<u8> {
        &self.0
    }
}

impl Deref for BytesKey {
    type Target = Vec<u8>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl From<Vec<u8>> for BytesKey {
    fn from(bz: Vec<u8>) -> Self {
        BytesKey(bz)
    }
}

impl From<&[u8]> for BytesKey {
    fn from(s: &[u8]) -> Self {
        Self(s.to_vec())
    }
}

impl From<&str> for BytesKey {
    fn from(s: &str) -> Self {
        Self::from(s.as_bytes())
    }
}

/// Custom trait to avoid issues like https://github.com/rust-lang/rust/issues/27108.
pub trait Hash {
    fn hash<H: Hasher>(&self, state: &mut H);

    fn hash_slice<H: Hasher>(data: &[Self], state: &mut H)
    where
        Self: Sized,
    {
        for piece in data {
            piece.hash(state);
        }
    }
}

macro_rules! impl_write {
    ($(($ty:ident, $meth:ident),)*) => {$(
        impl Hash for $ty {
            fn hash<H: Hasher>(&self, state: &mut H) {
                state.$meth(*self)
            }

            fn hash_slice<H: Hasher>(data: &[$ty], state: &mut H) {
                let newlen = data.len() * mem::size_of::<$ty>();
                let ptr = data.as_ptr() as *const u8;
                state.write(unsafe { slice::from_raw_parts(ptr, newlen) })
            }
        }
    )*}
}

impl_write! {
    (u8, write_u8),
    (u16, write_u16),
    (u32, write_u32),
    (u64, write_u64),
    (usize, write_usize),
    (i8, write_i8),
    (i16, write_i16),
    (i32, write_i32),
    (i64, write_i64),
    (isize, write_isize),
    (u128, write_u128),
    (i128, write_i128),
}

impl Hash for bool {
    fn hash<H: Hasher>(&self, state: &mut H) {
        state.write_u8(*self as u8)
    }
}

impl Hash for char {
    fn hash<H: Hasher>(&self, state: &mut H) {
        state.write_u32(*self as u32)
    }
}

impl Hash for str {
    fn hash<H: Hasher>(&self, state: &mut H) {
        state.write(self.as_bytes());
    }
}

impl Hash for String {
    fn hash<H: Hasher>(&self, state: &mut H) {
        state.write(self.as_bytes());
    }
}

macro_rules! impl_hash_tuple {
    () => (
        impl Hash for () {
            fn hash<H: Hasher>(&self, _state: &mut H) {}
        }
    );

    ( $($name:ident)+) => (
        impl<$($name: Hash),*> Hash for ($($name,)*) where last_type!($($name,)+): ?Sized {
            #[allow(non_snake_case)]
            fn hash<S: Hasher>(&self, state: &mut S) {
                let ($(ref $name,)*) = *self;
                $($name.hash(state);)*
            }
        }
    );
}

macro_rules! last_type {
    ($a:ident,) => { $a };
    ($a:ident, $($rest_a:ident,)+) => { last_type!($($rest_a,)+) };
}

impl_hash_tuple! {}
impl_hash_tuple! { A }
impl_hash_tuple! { A B }
impl_hash_tuple! { A B C }
impl_hash_tuple! { A B C D }
impl_hash_tuple! { A B C D E }
impl_hash_tuple! { A B C D E F }
impl_hash_tuple! { A B C D E F G }
impl_hash_tuple! { A B C D E F G H }
impl_hash_tuple! { A B C D E F G H I }
impl_hash_tuple! { A B C D E F G H I J }
impl_hash_tuple! { A B C D E F G H I J K }
impl_hash_tuple! { A B C D E F G H I J K L }

impl<T: Hash> Hash for [T] {
    fn hash<H: Hasher>(&self, state: &mut H) {
        Hash::hash_slice(self, state)
    }
}

impl<T: Hash> Hash for Vec<T> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        Hash::hash_slice(self, state)
    }
}

impl<T: ?Sized + Hash> Hash for &T {
    fn hash<H: Hasher>(&self, state: &mut H) {
        (**self).hash(state);
    }
}

impl<T: ?Sized + Hash> Hash for &mut T {
    fn hash<H: Hasher>(&self, state: &mut H) {
        (**self).hash(state);
    }
}

impl<T: ?Sized> Hash for *const T {
    fn hash<H: Hasher>(&self, state: &mut H) {
        if mem::size_of::<Self>() == mem::size_of::<usize>() {
            // Thin pointer
            state.write_usize(*self as *const () as usize);
        } else {
            // Fat pointer
            let (a, b) = unsafe { *(self as *const Self as *const (usize, usize)) };
            state.write_usize(a);
            state.write_usize(b);
        }
    }
}

impl<T: ?Sized> Hash for *mut T {
    fn hash<H: Hasher>(&self, state: &mut H) {
        if mem::size_of::<Self>() == mem::size_of::<usize>() {
            // Thin pointer
            state.write_usize(*self as *const () as usize);
        } else {
            // Fat pointer
            let (a, b) = unsafe { *(self as *const Self as *const (usize, usize)) };
            state.write_usize(a);
            state.write_usize(b);
        }
    }
}

/// Helper struct which indexes and allows returning bits from a hashed key
#[derive(Debug, Clone, Copy)]
pub struct HashBits<'a> {
    b: &'a HashedKey,
    pub consumed: u32,
}

fn mkmask(n: u32) -> u32 {
    ((1u64 << n) - 1) as u32
}

impl<'a> HashBits<'a> {
    pub fn new(hash_buffer: &'a HashedKey) -> HashBits<'a> {
        Self::new_at_index(hash_buffer, 0)
    }

    /// Constructs hash bits with custom consumed index
    pub fn new_at_index(hash_buffer: &'a HashedKey, consumed: u32) -> HashBits<'a> {
        Self {
            b: hash_buffer,
            consumed,
        }
    }

    /// Returns next `i` bits of the hash and returns the value as an integer and returns
    /// Error when maximum depth is reached
    pub fn next(&mut self, i: u32) -> Result<u32, Error> {
        if i > 8 {
            return Err(Error::InvalidHashBitLen);
        }
        if (self.consumed + i) as usize > self.b.len() * 8 {
            return Err(Error::MaxDepth);
        }
        Ok(self.next_bits(i))
    }

    fn next_bits(&mut self, i: u32) -> u32 {
        let curbi = self.consumed / 8;
        let leftb = 8 - (self.consumed % 8);

        let curb = self.b[curbi as usize] as u32;
        match i.cmp(&leftb) {
            Ordering::Equal => {
                // bits to consume is equal to the bits remaining in the currently indexed byte
                let out = mkmask(i) & curb;
                self.consumed += i;
                out
            }
            Ordering::Less => {
                // Consuming less than the remaining bits in the current byte
                let a = curb & mkmask(leftb);
                let b = a & !mkmask(leftb - i);
                let c = b >> (leftb - i);
                self.consumed += i;
                c
            }
            Ordering::Greater => {
                // Consumes remaining bits and remaining bits from a recursive call
                let mut out = (mkmask(leftb) & curb) as u64;
                out <<= i - leftb;
                self.consumed += leftb;
                out += self.next_bits(i - leftb) as u64;
                out as u32
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bitfield() {
        let mut key: HashedKey = Default::default();
        key[0] = 0b10001000;
        key[1] = 0b10101010;
        key[2] = 0b10111111;
        key[3] = 0b11111111;
        let mut hb = HashBits::new(&key);
        // Test eq cmp
        assert_eq!(hb.next(8).unwrap(), 0b10001000);
        // Test lt cmp
        assert_eq!(hb.next(5).unwrap(), 0b10101);
        // Test gt cmp
        assert_eq!(hb.next(5).unwrap(), 0b01010);
        assert_eq!(hb.next(6).unwrap(), 0b111111);
        assert_eq!(hb.next(8).unwrap(), 0b11111111);
        assert!(matches!(hb.next(9), Err(Error::InvalidHashBitLen)));
        for _ in 0..28 {
            // Iterate through rest of key to test depth
            hb.next(8).unwrap();
        }
        assert!(matches!(hb.next(1), Err(Error::MaxDepth)));
    }
}
