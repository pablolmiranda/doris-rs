//! `aes_encrypt`/`aes_decrypt`, `sm4_encrypt`/`sm4_decrypt`, `sm3`/`sm3sum`, `to_base64` and
//! `from_base64` as the C++ BE computes them (`function_encryption.cpp`,
//! `util/encryption_util.cpp`, `function_string_digest.cpp`, `function_string.cpp`): see
//! `docs/superpowers/research/2026-11-12-m6-s7b6-crypto.md`.
//!
//! The ported modes are AES-128/192/256 in ECB, CBC and GCM and SM4-128 in ECB and CBC, the ones
//! the recorded suites use. The other modes C++ knows (CFB, CFB1, CFB8, CFB128, OFB, CTR) are
//! refused by name; a mode C++ does not know is NULL, as there.
use aes::cipher::generic_array::GenericArray;
use aes::cipher::{BlockDecrypt, BlockEncrypt, KeyInit};
use aes_gcm::aead::{Aead, Payload};
use base64::Engine;
use doris_segment::writer::Datum;
use doris_thrift::gen::types::TPrimitiveType as T;

use crate::value::ColType;
use crate::ExecError;

const NAMES: [&str; 8] = [
    "aes_encrypt",
    "aes_decrypt",
    "sm4_encrypt",
    "sm4_decrypt",
    "sm3",
    "sm3sum",
    "to_base64",
    "from_base64",
];

pub fn is_crypto_function(name: &str) -> bool {
    NAMES.contains(&name)
}

fn text(t: &ColType) -> bool {
    matches!(t.ptype, T::CHAR | T::VARCHAR | T::STRING)
}

pub fn supported(name: &str, args: &[ColType], to: &ColType) -> bool {
    let arity = match name {
        // the FE always sends (s, key, iv, mode) or, for aes, (s, key, iv, mode, aad)
        "aes_encrypt" | "aes_decrypt" => (4..=5).contains(&args.len()),
        "sm4_encrypt" | "sm4_decrypt" => args.len() == 4,
        "sm3sum" => !args.is_empty(),
        _ => args.len() == 1,
    };
    arity && args.iter().all(text) && text(to)
}

#[derive(Clone, Copy, PartialEq)]
enum Cipher {
    Aes(usize),
    Sm4,
}

#[derive(Clone, Copy, PartialEq)]
enum Mode {
    Ecb,
    Cbc,
    Gcm,
}

/// A mode string (case-insensitive, `enc.cpp:52-85`): `Ok(None)` when C++ does not know it (a
/// NULL row), an error when C++ knows it and doris-rs does not port it.
fn mode(name: &str, s: &[u8], aad: bool) -> Result<Option<(Cipher, Mode)>, ExecError> {
    let sm4 = name.starts_with("sm4");
    let upper = String::from_utf8_lossy(s).to_ascii_uppercase();
    let upper = match upper.as_str() {
        // the registration's default: GCM for the 5-argument form
        "" if sm4 => "SM4_128_ECB".to_string(),
        "" if aad => "AES_128_GCM".to_string(),
        "" => "AES_128_ECB".to_string(),
        u => u.to_string(),
    };
    let parts: Vec<&str> = upper.split('_').collect();
    let [family, bits, m] = parts.as_slice() else { return Ok(None) };
    let cipher = match (*family, *bits, sm4) {
        ("AES", "128", false) => Cipher::Aes(16),
        ("AES", "192", false) => Cipher::Aes(24),
        ("AES", "256", false) => Cipher::Aes(32),
        ("SM4", "128", true) => Cipher::Sm4,
        _ => return Ok(None),
    };
    let known = match cipher {
        Cipher::Aes(_) => ["ECB", "CBC", "CFB", "CFB1", "CFB8", "CFB128", "CTR", "OFB", "GCM"].contains(m),
        Cipher::Sm4 => ["ECB", "CBC", "CFB128", "OFB", "CTR"].contains(m),
    };
    Ok(match *m {
        "ECB" => Some((cipher, Mode::Ecb)),
        "CBC" => Some((cipher, Mode::Cbc)),
        "GCM" if cipher != Cipher::Sm4 => Some((cipher, Mode::Gcm)),
        _ if known => {
            return Err(ExecError::Unsupported {
                what: format!("{name} in mode {upper}"),
            })
        }
        _ => None,
    })
}

/// Whether a write path may run this call: an encrypt or decrypt's mode must be a literal that
/// is not refused (a refusal there would only filter rows, where C++ fails the load).
pub fn writable(name: &str, mode_literal: Option<&[u8]>, aad: bool) -> bool {
    if !matches!(name, "aes_encrypt" | "aes_decrypt" | "sm4_encrypt" | "sm4_decrypt") {
        return true;
    }
    mode_literal.is_some_and(|m| mode(name, m, aad).is_ok())
}

/// MySQL's `my_aes_create_key`: the key's bytes XORed into a buffer of the cipher's size.
fn fold_key(key: &[u8], len: usize) -> Vec<u8> {
    let mut k = vec![0u8; len];
    for (i, b) in key.iter().enumerate() {
        k[i % len] ^= b;
    }
    k
}

/// The iv's first 16 bytes over "DORISDORISDORIS_" (`encryption_util.cpp:227-238`).
fn iv(iv: &[u8]) -> [u8; 16] {
    let mut buf = *b"DORISDORISDORIS_";
    let n = iv.len().min(16);
    buf[..n].copy_from_slice(&iv[..n]);
    buf
}

/// One 16-byte block cipher, both ways.
enum Block {
    Aes128(aes::Aes128),
    Aes192(aes::Aes192),
    Aes256(aes::Aes256),
    Sm4(sm4::Sm4),
}

impl Block {
    fn new(cipher: Cipher, key: &[u8]) -> Block {
        match cipher {
            Cipher::Aes(16) => Block::Aes128(aes::Aes128::new(GenericArray::from_slice(&fold_key(key, 16)))),
            Cipher::Aes(24) => Block::Aes192(aes::Aes192::new(GenericArray::from_slice(&fold_key(key, 24)))),
            Cipher::Aes(_) => Block::Aes256(aes::Aes256::new(GenericArray::from_slice(&fold_key(key, 32)))),
            Cipher::Sm4 => Block::Sm4(sm4::Sm4::new(GenericArray::from_slice(&fold_key(key, 16)))),
        }
    }

    fn encrypt(&self, b: &mut [u8]) {
        let b = GenericArray::from_mut_slice(b);
        match self {
            Block::Aes128(c) => c.encrypt_block(b),
            Block::Aes192(c) => c.encrypt_block(b),
            Block::Aes256(c) => c.encrypt_block(b),
            Block::Sm4(c) => c.encrypt_block(b),
        }
    }

    fn decrypt(&self, b: &mut [u8]) {
        let b = GenericArray::from_mut_slice(b);
        match self {
            Block::Aes128(c) => c.decrypt_block(b),
            Block::Aes192(c) => c.decrypt_block(b),
            Block::Aes256(c) => c.decrypt_block(b),
            Block::Sm4(c) => c.decrypt_block(b),
        }
    }
}

/// ECB or CBC with PKCS#7 padding, as EVP with padding on.
fn block_encrypt(c: &Block, cbc: Option<[u8; 16]>, s: &[u8]) -> Vec<u8> {
    let pad = 16 - s.len() % 16;
    let mut out = s.to_vec();
    out.extend(std::iter::repeat_n(pad as u8, pad));
    let mut prev = cbc;
    for chunk in out.chunks_mut(16) {
        if let Some(p) = prev {
            chunk.iter_mut().zip(p).for_each(|(b, x)| *b ^= x);
        }
        c.encrypt(chunk);
        if prev.is_some() {
            prev = Some(chunk.try_into().expect("a 16-byte block"));
        }
    }
    out
}

/// The inverse; None for a length that is not whole blocks (none at all included) or bad padding.
fn block_decrypt(c: &Block, cbc: Option<[u8; 16]>, s: &[u8]) -> Option<Vec<u8>> {
    if s.is_empty() || !s.len().is_multiple_of(16) {
        return None;
    }
    let mut out = s.to_vec();
    let mut prev = cbc;
    for chunk in out.chunks_mut(16) {
        let this: [u8; 16] = chunk.try_into().expect("a 16-byte block");
        c.decrypt(chunk);
        if let Some(p) = prev {
            chunk.iter_mut().zip(p).for_each(|(b, x)| *b ^= x);
            prev = Some(this);
        }
    }
    let pad = *out.last()? as usize;
    if !(1..=16).contains(&pad) || out[out.len() - pad..].iter().any(|b| *b as usize != pad) {
        return None;
    }
    out.truncate(out.len() - pad);
    Some(out)
}

/// GCM with a 12-byte iv: `iv || ciphertext || tag(16)` out; decrypt reads the iv from the input.
fn gcm(encrypt: bool, bytes: usize, key: &[u8], ivbuf: [u8; 16], s: &[u8], aad: &[u8]) -> Option<Vec<u8>> {
    fn run<C: Aead + KeyInit>(encrypt: bool, key: &[u8], ivbuf: [u8; 16], s: &[u8], aad: &[u8]) -> Option<Vec<u8>> {
        let c = C::new_from_slice(key).ok()?;
        if encrypt {
            let nonce = GenericArray::from_slice(&ivbuf[..12]);
            let mut out = ivbuf[..12].to_vec();
            out.extend(c.encrypt(nonce, Payload { msg: s, aad }).ok()?);
            Some(out)
        } else {
            if s.len() < 28 {
                return None;
            }
            let nonce = GenericArray::from_slice(&s[..12]);
            c.decrypt(nonce, Payload { msg: &s[12..], aad }).ok()
        }
    }
    let k = fold_key(key, bytes);
    match bytes {
        16 => run::<aes_gcm::Aes128Gcm>(encrypt, &k, ivbuf, s, aad),
        24 => run::<aes_gcm::AesGcm<aes::Aes192, aes_gcm::aead::consts::U12>>(encrypt, &k, ivbuf, s, aad),
        _ => run::<aes_gcm::Aes256Gcm>(encrypt, &k, ivbuf, s, aad),
    }
}

/// One encrypt or decrypt row; None is a NULL row.
fn crypt(name: &str, a: &[&[u8]]) -> Result<Option<Vec<u8>>, ExecError> {
    let aad = a.get(4).copied();
    let Some((cipher, m)) = mode(name, a[3], aad.is_some())? else {
        return Ok(None);
    };
    let encrypt = name.ends_with("encrypt");
    let ivbuf = iv(a[2]);
    Ok(match (cipher, m) {
        (Cipher::Aes(bytes), Mode::Gcm) => gcm(encrypt, bytes, a[1], ivbuf, a[0], aad.unwrap_or_default()),
        _ => {
            let c = Block::new(cipher, a[1]);
            let chain = (m == Mode::Cbc).then_some(ivbuf);
            if encrypt {
                Some(block_encrypt(&c, chain, a[0]))
            } else {
                block_decrypt(&c, chain, a[0])
            }
        }
    })
}

/// aklomp/base64's decoder: the standard alphabet, padded; non-zero trailing bits pass.
fn from_base64(s: &[u8]) -> Option<Vec<u8>> {
    let config = base64::engine::GeneralPurposeConfig::new()
        .with_decode_allow_trailing_bits(true)
        .with_decode_padding_mode(base64::engine::DecodePaddingMode::RequireCanonical);
    base64::engine::GeneralPurpose::new(&base64::alphabet::STANDARD, config).decode(s).ok()
}

pub fn eval(name: &str, args: &[&[Datum]], rows: usize) -> Result<Vec<Datum>, ExecError> {
    (0..rows)
        .map(|r| {
            let mut row = Vec::with_capacity(args.len());
            for a in args {
                match &a[r] {
                    Datum::Bytes(b) => row.push(b.as_slice()),
                    // any NULL argument is a NULL row
                    _ => return Ok(Datum::Null),
                }
            }
            let out = match name {
                "to_base64" => Some(base64::engine::general_purpose::STANDARD.encode(row[0]).into_bytes()),
                "from_base64" => from_base64(row[0]),
                // one digest over the arguments joined, in lower-case hex
                "sm3" | "sm3sum" => {
                    use sm3::Digest;
                    let mut h = sm3::Sm3::new();
                    for b in &row {
                        h.update(b);
                    }
                    Some(h.finalize().iter().map(|b| format!("{b:02x}")).collect::<String>().into_bytes())
                }
                _ => crypt(name, &row)?,
            };
            Ok(out.map_or(Datum::Null, Datum::Bytes))
        })
        .collect()
}
