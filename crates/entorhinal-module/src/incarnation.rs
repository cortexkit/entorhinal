//! A process-local invalidation nonce, never stored with identity or replies.

/// Draw the same eight OS-random bytes used for identity tokens. Entropy failure
/// must stop startup: a predictable fallback could reuse an old fleet token.
pub(super) fn new_incarnation() -> Result<String, getrandom::Error> {
    let mut bytes = [0u8; 8];
    getrandom::getrandom(&mut bytes)?;
    Ok(bytes.iter().map(|byte| format!("{byte:02x}")).collect())
}
