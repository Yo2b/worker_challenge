use std::{ffi, str};

/// A convenient helper function to encode `[u8; N]` arrays of any length into a valid OS string.
///
/// _Note: it only aims to provide the expected interface and a generic implementation rather than OS-specific,
/// fully optimized implementation._
///
/// # Panics
/// This function assumes `id` is only made of valid UTF-8 chars as `u8` and will panic otherwise.
pub fn encode_id<const N: usize>(id: &[u8; N]) -> &ffi::OsStr {
    str::from_utf8(id).unwrap().as_ref()
}

/// A convenient helper function to decode `[u8; N]` arrays of any length from a valid OS string.
///
/// _Note: it only aims to provide the expected interface and a generic implementation rather than OS-specific,
/// fully optimized implementation._
///
/// # Panics
/// This function assumes `id` is only made of valid UTF-8 chars as `u8` and will panic otherwise.
/// Moreover, `id` must have the exact expected length.
pub fn decode_id<const N: usize>(id: &ffi::OsStr) -> [u8; N] {
    let bytes = id.to_str().unwrap().as_bytes();
    match bytes.split_first_chunk::<N>() {
        Some((&id, [])) => id,
        _ => todo!("{id:?}"),
    }
}
