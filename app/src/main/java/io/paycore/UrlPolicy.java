package io.paycore;

import java.net.InetAddress;
import java.net.URI;
import java.net.UnknownHostException;
import java.util.Locale;
import org.springframework.stereotype.Component;

/**
 * Webhook destination policy (SSRF defence).
 *  - Default (strict): only http/https URLs whose host is in the explicit allowlist (paycore.webhooks.allowed-hosts). No
 *    credentials in the URL. Everything else is rejected, including raw IPs and any other hostname.
 *  - Optional (allow-public-hosts=true): other hosts are accepted only if EVERY address they resolve to is public. Blocked:
 *    loopback, link-local (169.254/16, includes cloud metadata 169.254.169.254), site-local (10/8, 172.16/12, 192.168/16),
 *    CGNAT 100.64/10, unspecified, multicast, IPv6 unique-local fc00::/7 and IPv4-mapped forms of those.
 * The check runs at registration AND again immediately before every delivery (hostnames can change).
 * Production notes: DNS rebinding means a name can resolve to a public address at check time and an internal one at connect time.
 * A production system should resolve once, validate, and connect to that exact IP (pinning, with the Host header/SNI set to the
 * name), forbid redirects, send webhooks from an egress proxy in an isolated network segment, and use short timeouts.
 * Redirects are disabled in this implementation; IP pinning is NOT implemented (documented limitation).
 */
@Component
public class UrlPolicy {
    private final Props props;

    public UrlPolicy(Props props) {
        this.props = props;
    }

    public URI validate(String url) {
        URI uri;
        try {
            uri = URI.create(url);
        } catch (IllegalArgumentException e) {
            throw ApiException.badRequest("webhook url is not a valid URI");
        }
        String scheme = uri.getScheme() == null ? "" : uri.getScheme().toLowerCase(Locale.ROOT);
        if (!scheme.equals("http") && !scheme.equals("https")) {
            throw ApiException.badRequest("webhook url must use http or https");
        }
        if (uri.getUserInfo() != null) {
            throw ApiException.badRequest("webhook url must not contain credentials");
        }
        String host = uri.getHost();
        if (host == null) {
            throw ApiException.badRequest("webhook url must have a host");
        }
        boolean allowlisted = props.webhooks().allowedHosts().stream().anyMatch(h -> h.equalsIgnoreCase(host));
        if (allowlisted) {
            return uri;
        }
        if (!props.webhooks().allowPublicHosts()) {
            throw ApiException.badRequest("webhook host is not in the allowlist");
        }
        try {
            for (InetAddress a : InetAddress.getAllByName(host)) {
                if (isBlocked(a)) {
                    throw ApiException.badRequest("webhook host resolves to a non-public address");
                }
            }
        } catch (UnknownHostException e) {
            throw ApiException.badRequest("webhook host cannot be resolved");
        }
        return uri;
    }

    public static boolean isBlocked(InetAddress a) {
        if (a.isAnyLocalAddress() || a.isLoopbackAddress() || a.isLinkLocalAddress() || a.isSiteLocalAddress() || a.isMulticastAddress()) {
            return true;
        }
        byte[] b = a.getAddress();
        if (b.length == 4) {
            int first = b[0] & 0xff;
            int second = b[1] & 0xff;
            return (first == 100 && second >= 64 && second <= 127)   // CGNAT 100.64.0.0/10
                    || first == 0 || first >= 240;                    // "this network", reserved/broadcast
        }
        return (b[0] & 0xfe) == 0xfc;                                 // IPv6 unique local fc00::/7
    }
}
