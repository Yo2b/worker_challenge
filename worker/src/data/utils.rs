use std::{ffi, str};

pub fn encode_id<const N: usize>(id: &[u8; N]) -> &ffi::OsStr {
    str::from_utf8(id).unwrap().as_ref() // assume `id` is valid UTF-8
}

pub fn decode_id<const N: usize>(id: &ffi::OsStr) -> [u8; N] {
    let bytes = id.to_str().unwrap().as_bytes(); // assume `id` is valid UTF-8
    match bytes.split_first_chunk::<N>() {
        Some((&id, [])) => id,
        _ => todo!("{id:?}"),
    }
}
