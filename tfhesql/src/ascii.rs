// Little endian
#[inline(always)]
pub fn ascii_to_le_u64x4(s: &str) -> [u64; 4] {
    let mut four_u64 = [0u64; 4];

    let s_len = s.len();
    let s_bytes = s.as_bytes();

    let mut le_bytes_0: [u8; 8] = [0; 8];
    let mut le_bytes_1: [u8; 8] = [0; 8];
    let mut le_bytes_2: [u8; 8] = [0; 8];
    let mut le_bytes_3: [u8; 8] = [0; 8];

    let len = 8.min(s_len);
    le_bytes_0[..len].copy_from_slice(&s_bytes[0..len]);

    if s_len > 8 {
        let len = 8.min(s_len-8);
        le_bytes_1[..len].copy_from_slice(&s_bytes[8..(8+len)]);

        if s_len > 16 {
            let len = 8.min(s_len-16);
            le_bytes_2[..len].copy_from_slice(&s_bytes[16..(16+len)]);

            if s_len > 24 {
                let len = 8.min(s_len-24);
                le_bytes_3[..len].copy_from_slice(&s_bytes[24..(24+len)]);
            }
        }
    }

    four_u64[0] = u64::from_le_bytes(le_bytes_0);
    four_u64[1] = u64::from_le_bytes(le_bytes_1);
    four_u64[2] = u64::from_le_bytes(le_bytes_2);
    four_u64[3] = u64::from_le_bytes(le_bytes_3);

    four_u64
}

// Little endian
#[cfg(test)]
#[inline(always)]
pub fn ascii_to_le_u64(s: &str) -> u64 {
    let mut le_bytes: [u8; 8] = [0; 8];
    let len = 8.min(s.len());
    le_bytes[..len].copy_from_slice(&s.as_bytes()[..len]);
    u64::from_le_bytes(le_bytes)
}

#[cfg(test)]
pub fn rand_gen_ascii_32(rng: &mut rand::rngs::ThreadRng) -> String {
    use rand::Rng;
    let n = rng.gen_range(1..=32);
    let u8x32: [u8; 32] = std::array::from_fn(|i| {
        if i < n {
            // 32 = Space
            // 126 = ~
            rng.gen_range(32..=126)
        } else {
            0
        }
    });
    le_u8x32_to_string(&u8x32)
}

// Little endian
pub fn le_u8x32_to_string(u8x32: &[u8; 32]) -> String {
    String::from_utf8(u8x32.to_vec())
        .unwrap()
        .trim_matches(char::from(0))
        .to_string()
}

// Little endian
#[cfg(test)]
pub fn le_u64_to_string(u64x1: &u64) -> String {
    String::from_utf8(u64x1.to_le_bytes().to_vec())
        .unwrap()
        .trim_matches(char::from(0))
        .to_string()
}

// Little endian
#[cfg(test)]
pub fn le_u128x2_to_string(u128x2: &[u128; 2]) -> String {
    use crate::uint::traits::ToLeU8;
    String::from_utf8(u128x2.to_le_u8().to_vec())
        .unwrap()
        .trim_matches(char::from(0))
        .to_string()
}

#[cfg(test)]
pub fn le_u64x4_to_string(u64x4: &[u64; 4]) -> String {
    let s0 = le_u64_to_string(&u64x4[0]);
    let s1 = le_u64_to_string(&u64x4[1]);
    let s2 = le_u64_to_string(&u64x4[2]);
    let s3 = le_u64_to_string(&u64x4[3]);
    format!("{}{}{}{}", s0, s1, s2, s3)
}

pub fn ascii_to_le_u8x32(s: &str) -> [u8; 32] {
    let len = 32.min(s.len());
    let mut le_bytes: [u8; 32] = [0; 32];
    s.as_bytes()
        .iter()
        .take(len)
        .enumerate()
        .for_each(|(i, c)| {
            le_bytes[i] = *c;
        });
    le_bytes
}

#[cfg(test)]
pub fn ascii_to_le_u128x2(s: &str) -> [u128; 2] {
    let len = 32.min(s.len());
    let mut le_bytes: [u8; 32] = [0; 32];
    s.as_bytes()
        .iter()
        .take(len)
        .enumerate()
        .for_each(|(i, c)| {
            le_bytes[i] = *c;
        });
    [
        u128::from_le_bytes(le_bytes[..16].try_into().unwrap()),
        u128::from_le_bytes(le_bytes[16..].try_into().unwrap()),
    ]
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_four_u64() {
        let s = "Hello World!  ";
        let four64 = ascii_to_le_u64x4(s);

        let mut s0 = le_u64_to_string(&four64[0]);
        let s1 = le_u64_to_string(&four64[1]);
        let s2 = le_u64_to_string(&four64[2]);
        let s3 = le_u64_to_string(&four64[3]);

        s0.push_str(s1.as_str());
        s0.push_str(s2.as_str());
        s0.push_str(s3.as_str());

        assert_eq!(s0, s);
    }

    #[test]
    fn test_u8x32() {
        let s = "Hello World!  ";
        let u8x32 = ascii_to_le_u8x32(s);

        let s0 = le_u8x32_to_string(&u8x32);
        assert_eq!(s0, s);
    }

    #[test]
    fn test_u128x2() {
        let s = "Hello World!  ";
        let u128x2 = ascii_to_le_u128x2(s);

        let s0 = le_u128x2_to_string(&u128x2);
        assert_eq!(s0, s);
    }

    #[test]
    fn test_ascii_to_le_u64() {
        let s = "Hello World!  ";
        let u64x1 = ascii_to_le_u64(s);
        let s0 = le_u64_to_string(&u64x1);
        assert_eq!(s0, s[0..8]);
    }

    #[test]
    fn test_u64x4() {
        let s = "Hello World!  ";
        let u64x4 = ascii_to_le_u64x4(s);

        let s0 = le_u64x4_to_string(&u64x4);
        assert_eq!(s0, s);
    }

    #[test]
    fn test_le_bytes() {
        let s = "Hell";
        let u: u32 = 1819043144_u32;
        let sb = s.as_bytes();
        assert_eq!(sb.len(), 4);
        let sb_4: [u8; 4] = [sb[0], sb[1], sb[2], sb[3]];
        assert_eq!(u32::from_le_bytes(sb_4), u);
    }
}
