package io.paycore;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

/**
 * Webhook signing. Header: PayCore-Signature: t=<unix seconds>,v1=<hex HMAC-SHA256(secret, t + "." + body)>.
 * Receivers must verify with a constant-time compare, reject timestamps outside a tolerance (replay defence), and
 * de-duplicate on the stable PayCore-Event-Id header: delivery is AT-LEAST-ONCE.
 */
public final class Webhook {
    private Webhook() {}

    public static String sign(String secret, long timestamp, String body) {
        try {
            Mac mac = Mac.getInstance("HmacSHA256");
            mac.init(new SecretKeySpec(secret.getBytes(StandardCharsets.UTF_8), "HmacSHA256"));
            return java.util.HexFormat.of().formatHex(mac.doFinal((timestamp + "." + body).getBytes(StandardCharsets.UTF_8)));
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    public static String header(String secret, long timestamp, String body) {
        return "t=" + timestamp + ",v1=" + sign(secret, timestamp, body);
    }

    public static boolean verify(String secret, String header, String body, long nowSeconds, long toleranceSeconds) {
        if (header == null) {
            return false;
        }
        String t = null;
        String v1 = null;
        for (String part : header.split(",")) {
            if (part.startsWith("t=")) t = part.substring(2);
            else if (part.startsWith("v1=")) v1 = part.substring(3);
        }
        if (t == null || v1 == null) {
            return false;
        }
        long ts;
        try {
            ts = Long.parseLong(t);
        } catch (NumberFormatException e) {
            return false;
        }
        if (Math.abs(nowSeconds - ts) > toleranceSeconds) {
            return false;
        }
        return MessageDigest.isEqual(sign(secret, ts, body).getBytes(StandardCharsets.UTF_8), v1.getBytes(StandardCharsets.UTF_8));
    }
}
