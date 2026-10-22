//! Exact decimal-string to fixed-point conversion.
//!
//! Exchanges send prices and sizes as decimal strings (`"0.53"`, `"1520.25"`).
//! Going through `f64` would silently introduce rounding error, so we parse the
//! digits directly into an integer scaled by [`SCALE`].

use anyhow::{Context, bail};

/// Fixed-point scale used by the output schema: `1_000_000 = 1 USDC` for
/// prices and `1_000_000 = 1 share/contract` for quantities.
pub const SCALE: i64 = 1_000_000;
const SCALE_DIGITS: usize = 6;

/// Parses a decimal string into fixed point with six decimal places.
///
/// Accepts an optional sign, and rejects anything that would lose precision
/// (more than six significant fractional digits) or overflow `i64`.
pub fn parse(text: &str) -> anyhow::Result<i64> {
    let text = text.trim();
    let (negative, digits) = match text.as_bytes().first() {
        Some(b'-') => (true, &text[1..]),
        Some(b'+') => (false, &text[1..]),
        _ => (false, text),
    };
    let (whole, frac) = digits.split_once('.').unwrap_or((digits, ""));
    if whole.is_empty() && frac.is_empty() {
        bail!("empty decimal `{text}`");
    }
    if !whole
        .bytes()
        .chain(frac.bytes())
        .all(|b| b.is_ascii_digit())
    {
        bail!("invalid decimal `{text}`");
    }

    let (kept, dropped) = frac.split_at(frac.len().min(SCALE_DIGITS));
    if dropped.bytes().any(|b| b != b'0') {
        bail!("decimal `{text}` has more than {SCALE_DIGITS} fractional digits");
    }

    let whole_value = if whole.is_empty() {
        0
    } else {
        whole
            .parse::<i64>()
            .with_context(|| format!("decimal `{text}` out of range"))?
    };
    let mut frac_value = if kept.is_empty() {
        0
    } else {
        kept.parse::<i64>()?
    };
    for _ in kept.len()..SCALE_DIGITS {
        frac_value *= 10;
    }

    let value = whole_value
        .checked_mul(SCALE)
        .and_then(|v| v.checked_add(frac_value))
        .with_context(|| format!("decimal `{text}` out of range"))?;
    Ok(if negative { -value } else { value })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_common_forms() {
        assert_eq!(parse("0.53").unwrap(), 530_000);
        assert_eq!(parse("1").unwrap(), 1_000_000);
        assert_eq!(parse(".5").unwrap(), 500_000);
        assert_eq!(parse("0.0001").unwrap(), 100);
        assert_eq!(parse("1520.25").unwrap(), 1_520_250_000);
        assert_eq!(parse("-12.00").unwrap(), -12_000_000);
        assert_eq!(parse("0.123456").unwrap(), 123_456);
        assert_eq!(parse("3.1000000000").unwrap(), 3_100_000);
    }

    #[test]
    fn rejects_lossy_or_invalid() {
        assert!(parse("0.1234567").is_err());
        assert!(parse("abc").is_err());
        assert!(parse("").is_err());
        assert!(parse("1e5").is_err());
        assert!(parse("99999999999999999").is_err());
    }
}
