pub mod rayon;
pub mod path;
pub mod arrow;

#[allow(dead_code)]
pub const fn is_little_endian() -> bool {
    u16::from_ne_bytes([1, 0]) == 1
}
