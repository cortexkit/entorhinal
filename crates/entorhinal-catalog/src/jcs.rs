//! RFC 8785 serialization with strict, recursive duplicate-key detection.
use serde::de::{self, MapAccess, SeqAccess, Visitor};
use serde::{Deserialize, Deserializer};
use serde_json::{Map, Number, Value};
use std::fmt;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum JsonError {
    InvalidJson,
    NumberOutOfRange,
}
impl fmt::Display for JsonError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for JsonError {}
struct Strict(Value, Vec<String>);
impl<'de> Deserialize<'de> for Strict {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        struct StrictVisitor;
        impl<'de> Visitor<'de> for StrictVisitor {
            type Value = Strict;
            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("JSON without duplicate keys")
            }
            fn visit_bool<E: de::Error>(self, v: bool) -> Result<Strict, E> {
                Ok(Strict(Value::Bool(v), Vec::new()))
            }
            fn visit_unit<E: de::Error>(self) -> Result<Strict, E> {
                Ok(Strict(Value::Null, Vec::new()))
            }
            fn visit_i64<E: de::Error>(self, v: i64) -> Result<Strict, E> {
                Ok(Strict(v.into(), Vec::new()))
            }
            fn visit_u64<E: de::Error>(self, v: u64) -> Result<Strict, E> {
                Ok(Strict(v.into(), Vec::new()))
            }
            fn visit_f64<E: de::Error>(self, v: f64) -> Result<Strict, E> {
                Number::from_f64(v)
                    .map(|n| Strict(Value::Number(n), Vec::new()))
                    .ok_or_else(|| E::custom("nonfinite number"))
            }
            fn visit_str<E: de::Error>(self, v: &str) -> Result<Strict, E> {
                Ok(Strict(v.into(), Vec::new()))
            }
            fn visit_string<E: de::Error>(self, v: String) -> Result<Strict, E> {
                Ok(Strict(v.into(), Vec::new()))
            }
            fn visit_seq<A: SeqAccess<'de>>(self, mut a: A) -> Result<Strict, A::Error> {
                let mut v = Vec::new();
                while let Some(Strict(x, _)) = a.next_element()? {
                    v.push(x);
                }
                Ok(Strict(Value::Array(v), Vec::new()))
            }
            fn visit_map<A: MapAccess<'de>>(self, mut a: A) -> Result<Strict, A::Error> {
                let mut m = Map::new();
                let mut source_keys = Vec::new();
                while let Some(k) = a.next_key::<String>()? {
                    if m.contains_key(&k) {
                        return Err(de::Error::custom("duplicate key"));
                    }
                    let Strict(v, _) = a.next_value()?;
                    source_keys.push(k.clone());
                    m.insert(k, v);
                }
                Ok(Strict(Value::Object(m), source_keys))
            }
        }
        d.deserialize_any(StrictVisitor)
    }
}
pub fn parse(text: &str) -> Result<Value, JsonError> {
    serde_json::from_str::<Strict>(text)
        .map(|v| v.0)
        .map_err(|_| JsonError::InvalidJson)
}
/// Parse a signed document without normalizing fractional, exponent or negative-zero tokens.
/// General JSON and RFC 8785 canonicalization still use `parse`.
pub fn parse_signed(text: &str) -> Result<Value, JsonError> {
    parse_signed_ordered(text).map(|v| v.0)
}
pub(crate) fn parse_signed_ordered(text: &str) -> Result<(Value, Vec<String>), JsonError> {
    if !integer_number_tokens(text) {
        return Err(JsonError::InvalidJson);
    }
    serde_json::from_str::<Strict>(text)
        .map(|v| (v.0, v.1))
        .map_err(|_| JsonError::InvalidJson)
}
fn integer_number_tokens(text: &str) -> bool {
    let bytes = text.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'"' => {
                i += 1;
                while i < bytes.len() && bytes[i] != b'"' {
                    if bytes[i] == b'\\' {
                        i += 1;
                    }
                    i += 1;
                }
                i += 1;
            }
            b'-' | b'0'..=b'9' => {
                let start = i;
                i += 1;
                while i < bytes.len()
                    && matches!(bytes[i], b'0'..=b'9' | b'.' | b'e' | b'E' | b'+' | b'-')
                {
                    i += 1;
                }
                let token = &bytes[start..i];
                if token == b"-0" || token.iter().any(|b| matches!(b, b'.' | b'e' | b'E')) {
                    return false;
                }
            }
            _ => i += 1,
        }
    }
    // Serde checks JSON grammar and duplicate keys after this lexical check.
    true
}
pub fn canonicalize(text: &str) -> Result<String, JsonError> {
    to_string(&parse(text)?)
}
pub fn to_string(v: &Value) -> Result<String, JsonError> {
    let mut out = String::new();
    emit(v, &mut out)?;
    Ok(out)
}
fn emit(v: &Value, out: &mut String) -> Result<(), JsonError> {
    match v {
        Value::Null => out.push_str("null"),
        Value::Bool(b) => out.push_str(if *b { "true" } else { "false" }),
        Value::String(s) => {
            out.push_str(&serde_json::to_string(s).map_err(|_| JsonError::InvalidJson)?)
        }
        Value::Number(n) => out.push_str(&number(n.as_f64().ok_or(JsonError::NumberOutOfRange)?)?),
        Value::Array(a) => {
            out.push('[');
            for (i, x) in a.iter().enumerate() {
                if i != 0 {
                    out.push(',');
                }
                emit(x, out)?;
            }
            out.push(']');
        }
        Value::Object(m) => {
            let mut keys: Vec<_> = m.keys().collect();
            keys.sort_by(|a, b| a.encode_utf16().cmp(b.encode_utf16()));
            out.push('{');
            for (i, k) in keys.iter().enumerate() {
                if i != 0 {
                    out.push(',');
                }
                emit(&Value::String((*k).clone()), out)?;
                out.push(':');
                emit(&m[*k], out)?;
            }
            out.push('}');
        }
    }
    Ok(())
}
// Find the shortest rounded decimal that round-trips to the binary64 input,
// then apply ECMAScript's fixed/exponential cutoffs (not Rust's Display cutoffs).
fn number(v: f64) -> Result<String, JsonError> {
    if !v.is_finite() {
        return Err(JsonError::NumberOutOfRange);
    }
    if v == 0.0 {
        return Ok("0".into());
    }
    let x = v.abs();
    let mut chosen = None;
    for precision in 0..17 {
        let s = format!("{x:.precision$e}");
        if s.parse::<f64>().ok() == Some(x) {
            chosen = Some(s);
            break;
        }
    }
    let s = chosen.ok_or(JsonError::NumberOutOfRange)?;
    let (mantissa, exponent) = s.split_once('e').ok_or(JsonError::NumberOutOfRange)?;
    let exponent: i32 = exponent.parse().map_err(|_| JsonError::NumberOutOfRange)?;
    let digits = mantissa.replace('.', "");
    let digits = digits.trim_end_matches('0');
    let k = digits.len() as i32;
    let n = exponent + 1;
    let body = if k <= n && n <= 21 {
        format!("{digits}{}", "0".repeat((n - k) as usize))
    } else if 0 < n && n <= 21 {
        format!("{}.{}", &digits[..n as usize], &digits[n as usize..])
    } else if -6 < n && n <= 0 {
        format!("0.{}{digits}", "0".repeat((-n) as usize))
    } else {
        let mantissa = if k == 1 {
            digits.to_string()
        } else {
            format!("{}.{}", &digits[..1], &digits[1..])
        };
        format!(
            "{mantissa}e{}{exponent}",
            if exponent >= 0 { "+" } else { "" }
        )
    };
    Ok(format!(
        "{}{body}",
        if v.is_sign_negative() { "-" } else { "" }
    ))
}
