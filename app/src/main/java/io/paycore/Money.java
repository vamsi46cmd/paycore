package io.paycore;

/**
 * Exact money: integer MINOR units (USD cents) plus a currency. No float/double anywhere.
 * Currency policy: USD only, scale 2, so 1 USD = 100 minor units and no rounding is ever needed because the API accepts
 * integer minor units only (there is nothing to round). Overflow is detected (Math.addExact / subtractExact).
 */
public record Money(long minor, String currency) {
    public static final String USD = "USD";
    /** Largest accepted single amount: $10,000,000.00. */
    public static final long MAX_MINOR = 1_000_000_000L;

    public Money {
        if (!USD.equals(currency)) {
            throw new IllegalArgumentException("unsupported currency: only USD");
        }
    }

    public static Money usd(long minor) {
        return new Money(minor, USD);
    }

    /** Validates an API-supplied amount: positive and within limits. */
    public static long requireValidAmount(long minor) {
        if (minor <= 0) {
            throw new IllegalArgumentException("amount must be positive");
        }
        if (minor > MAX_MINOR) {
            throw new IllegalArgumentException("amount exceeds the maximum of " + MAX_MINOR + " minor units");
        }
        return minor;
    }

    public static String requireCurrency(String currency) {
        if (!USD.equals(currency)) {
            throw new IllegalArgumentException("unsupported currency: only USD is supported");
        }
        return currency;
    }

    public Money plus(Money o) {
        sameCurrency(o);
        return new Money(Math.addExact(minor, o.minor), currency);
    }

    public Money minus(Money o) {
        sameCurrency(o);
        return new Money(Math.subtractExact(minor, o.minor), currency);
    }

    public boolean isPositive() {
        return minor > 0;
    }

    private void sameCurrency(Money o) {
        if (!currency.equals(o.currency)) {
            throw new IllegalArgumentException("currency mismatch");
        }
    }

    /** Display only, e.g. 12345 -> "123.45 USD". Integer arithmetic, no floating point. */
    @Override
    public String toString() {
        long abs = Math.abs(minor);
        return (minor < 0 ? "-" : "") + (abs / 100) + "." + String.format("%02d", abs % 100) + " " + currency;
    }
}
