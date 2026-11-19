package io.paycore;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.annotation.PostConstruct;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.List;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.boot.web.servlet.FilterRegistrationBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.config.http.SessionCreationPolicy;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.web.AuthenticationEntryPoint;
import org.springframework.security.web.SecurityFilterChain;
import org.springframework.security.web.access.AccessDeniedHandler;
import org.springframework.security.web.authentication.UsernamePasswordAuthenticationFilter;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

/**
 * Authentication: merchants present an API key (X-Api-Key); only its SHA-256 hash is stored. Operators present X-Admin-Key
 * (compared in constant time). Every merchant-facing query is scoped by the authenticated merchant id, and a resource that
 * belongs to another merchant is reported as 404, never 403.
 */
@Configuration
@EnableWebSecurity
public class SecurityConfig {
    public record MerchantPrincipal(UUID merchantId) {}

    private final Props props;

    public SecurityConfig(Props props) {
        this.props = props;
    }

    @PostConstruct
    void validate() {
        if (props.adminKey() == null || props.adminKey().length() < 16) {
            throw new IllegalStateException("ADMIN_KEY must be set and at least 16 characters long");
        }
    }

    public static String sha256(String s) {
        try {
            return java.util.HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(s.getBytes(StandardCharsets.UTF_8)));
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    static void writeError(ObjectMapper mapper, HttpServletResponse res, HttpStatus s, String code, String msg, String path) throws IOException {
        res.setStatus(s.value());
        res.setContentType(MediaType.APPLICATION_JSON_VALUE);
        mapper.writeValue(res.getOutputStream(), new ApiExceptionHandler.ApiError(java.time.Instant.now(), s.value(), code, msg, path, null));
    }

    public static class ApiKeyAuthFilter extends OncePerRequestFilter {
        private final JdbcTemplate jdbc;
        private final Props props;
        private final ObjectMapper mapper;

        public ApiKeyAuthFilter(JdbcTemplate jdbc, Props props, ObjectMapper mapper) {
            this.jdbc = jdbc;
            this.props = props;
            this.mapper = mapper;
        }

        @Override
        protected void doFilterInternal(HttpServletRequest req, HttpServletResponse res, FilterChain chain) throws ServletException, IOException {
            String admin = req.getHeader("X-Admin-Key");
            String key = req.getHeader("X-Api-Key");
            if (admin != null && MessageDigest.isEqual(admin.getBytes(StandardCharsets.UTF_8), props.adminKey().getBytes(StandardCharsets.UTF_8))) {
                SecurityContextHolder.getContext().setAuthentication(new UsernamePasswordAuthenticationToken("admin", null, List.of(new SimpleGrantedAuthority("ROLE_ADMIN"))));
            } else if (key != null && key.length() <= 128) {
                try {
                    List<UUID> ids = jdbc.queryForList("SELECT id FROM merchants WHERE api_key_hash = ?", UUID.class, sha256(key));
                    if (!ids.isEmpty()) {
                        SecurityContextHolder.getContext().setAuthentication(new UsernamePasswordAuthenticationToken(
                                new MerchantPrincipal(ids.get(0)), null, List.of(new SimpleGrantedAuthority("ROLE_MERCHANT"))));
                    }
                } catch (org.springframework.dao.DataAccessException | org.springframework.transaction.TransactionException e) {
                    // The key could not be CHECKED: that is a dependency outage, not a bad credential. Answering 401 would make a
                    // merchant believe a valid key was revoked, so answer 503 and tell the client to retry.
                    res.setHeader("Retry-After", "5");
                    writeError(mapper, res, HttpStatus.SERVICE_UNAVAILABLE, "service_unavailable", "Service temporarily unavailable; retry with the same Idempotency-Key", req.getRequestURI());
                    return;
                }
            }
            chain.doFilter(req, res);
        }
    }

    /** Fixed-window per-merchant limiter in Redis. It is a courtesy control: if Redis is down it FAILS OPEN; financial correctness never depends on it. */
    public static class RateLimitFilter extends OncePerRequestFilter {
        private static final Logger log = LoggerFactory.getLogger(RateLimitFilter.class);
        private final StringRedisTemplate redis;
        private final Props props;
        private final ObjectMapper mapper;
        private final Counter limited;
        private volatile long skipUntil;

        public RateLimitFilter(StringRedisTemplate redis, Props props, ObjectMapper mapper, MeterRegistry registry) {
            this.redis = redis;
            this.props = props;
            this.mapper = mapper;
            this.limited = registry.counter("paycore.ratelimit.limited");
        }

        @Override
        protected void doFilterInternal(HttpServletRequest req, HttpServletResponse res, FilterChain chain) throws ServletException, IOException {
            var auth = SecurityContextHolder.getContext().getAuthentication();
            if (auth != null && auth.getPrincipal() instanceof MerchantPrincipal p && System.currentTimeMillis() >= skipUntil) {
                try {
                    String k = "pc:rl:" + p.merchantId() + ":" + (System.currentTimeMillis() / 1000);
                    Long n = redis.opsForValue().increment(k);
                    if (n != null && n == 1) {
                        redis.expire(k, java.time.Duration.ofSeconds(3));
                    }
                    if (n != null && n > props.ratelimit().perSecond()) {
                        limited.increment();
                        res.setHeader("Retry-After", "1");
                        writeError(mapper, res, HttpStatus.TOO_MANY_REQUESTS, "rate_limited", "Rate limit exceeded", req.getRequestURI());
                        return;
                    }
                } catch (Exception e) {
                    skipUntil = System.currentTimeMillis() + 5000;   // do not pay a timeout per request while Redis is down
                    log.warn("Rate limiter unavailable, failing open: {}", e.toString());
                }
            }
            chain.doFilter(req, res);
        }
    }

    /** Rejects declared bodies over 64 KB. */
    public static class BodySizeFilter extends OncePerRequestFilter {
        private final ObjectMapper mapper;

        public BodySizeFilter(ObjectMapper mapper) {
            this.mapper = mapper;
        }

        @Override
        protected void doFilterInternal(HttpServletRequest req, HttpServletResponse res, FilterChain chain) throws ServletException, IOException {
            if (req.getContentLengthLong() > 65_536) {
                writeError(mapper, res, HttpStatus.PAYLOAD_TOO_LARGE, "payload_too_large", "Request body exceeds 64 KB", req.getRequestURI());
                return;
            }
            chain.doFilter(req, res);
        }
    }

    @Component
    @Order(Ordered.HIGHEST_PRECEDENCE)
    public static class RequestIdFilter extends OncePerRequestFilter {
        @Override
        protected void doFilterInternal(HttpServletRequest req, HttpServletResponse res, FilterChain chain) throws ServletException, IOException {
            String id = req.getHeader("X-Request-Id");
            if (id == null || !id.matches("[A-Za-z0-9_-]{1,64}")) {
                id = UUID.randomUUID().toString();
            }
            MDC.put("requestId", id);
            res.setHeader("X-Request-Id", id);
            try {
                chain.doFilter(req, res);
            } finally {
                MDC.remove("requestId");
            }
        }
    }

    @Bean ApiKeyAuthFilter apiKeyAuthFilter(JdbcTemplate jdbc, ObjectMapper mapper) { return new ApiKeyAuthFilter(jdbc, props, mapper); }

    @Bean RateLimitFilter rateLimitFilter(StringRedisTemplate redis, ObjectMapper mapper, MeterRegistry registry) { return new RateLimitFilter(redis, props, mapper, registry); }

    @Bean BodySizeFilter bodySizeFilter(ObjectMapper mapper) { return new BodySizeFilter(mapper); }

    @Bean FilterRegistrationBean<ApiKeyAuthFilter> noAutoAuth(ApiKeyAuthFilter f) { var r = new FilterRegistrationBean<>(f); r.setEnabled(false); return r; }

    @Bean FilterRegistrationBean<RateLimitFilter> noAutoRate(RateLimitFilter f) { var r = new FilterRegistrationBean<>(f); r.setEnabled(false); return r; }

    @Bean FilterRegistrationBean<BodySizeFilter> noAutoBody(BodySizeFilter f) { var r = new FilterRegistrationBean<>(f); r.setEnabled(false); return r; }

    @Bean
    SecurityFilterChain chain(HttpSecurity http, ApiKeyAuthFilter auth, RateLimitFilter rate, BodySizeFilter body, ObjectMapper mapper) throws Exception {
        AuthenticationEntryPoint entry = (req, res, ex) -> writeError(mapper, res, HttpStatus.UNAUTHORIZED, "unauthorized", "Valid API key required", req.getRequestURI());
        AccessDeniedHandler denied = (req, res, ex) -> writeError(mapper, res, HttpStatus.FORBIDDEN, "forbidden", "Not permitted", req.getRequestURI());
        http.csrf(c -> c.disable())   // stateless API-key authentication, no cookies
            .sessionManagement(s -> s.sessionCreationPolicy(SessionCreationPolicy.STATELESS))
            .authorizeHttpRequests(a -> a
                .requestMatchers("/api/v1/docs/**", "/api/v1/openapi/**", "/swagger-ui/**", "/swagger-ui.html", "/actuator/**").permitAll()
                .requestMatchers("/internal/test-receiver/**").permitAll()          // verifies the webhook HMAC itself; development only
                .requestMatchers(HttpMethod.POST, "/api/v1/merchants").hasRole("ADMIN")
                .requestMatchers("/api/v1/reconciliation/**").hasRole("ADMIN")
                .requestMatchers("/api/v1/**").hasRole("MERCHANT")
                .anyRequest().denyAll())
            .exceptionHandling(e -> e.authenticationEntryPoint(entry).accessDeniedHandler(denied))
            .addFilterBefore(body, UsernamePasswordAuthenticationFilter.class)
            .addFilterBefore(auth, UsernamePasswordAuthenticationFilter.class)
            .addFilterAfter(rate, ApiKeyAuthFilter.class);
        return http.build();
    }
}
