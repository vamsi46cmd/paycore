package io.paycore;

import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Size;
import java.security.SecureRandom;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.http.HttpStatus;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/v1")
@ConditionalOnProperty(name = "paycore.roles.api", havingValue = "true")
public class OtherControllers {
    public record CreateMerchant(@NotBlank @Size(max = 100) String name) {}

    private final JdbcTemplate jdbc;
    private final PaymentService payments;
    private final Ledger ledger;
    private final SecureRandom random = new SecureRandom();

    public OtherControllers(JdbcTemplate jdbc, PaymentService payments, Ledger ledger) {
        this.jdbc = jdbc;
        this.payments = payments;
        this.ledger = ledger;
    }

    /** Operator-only (X-Admin-Key). The API key is returned ONCE; only its SHA-256 hash is stored. */
    @PostMapping("/merchants")
    @ResponseStatus(HttpStatus.CREATED)
    @Tag(name = "Merchants")
    public Map<String, Object> createMerchant(@Valid @RequestBody CreateMerchant req) {
        byte[] raw = new byte[24];
        random.nextBytes(raw);
        String apiKey = "pk_" + HexFormat.of().formatHex(raw);
        UUID id = UUID.randomUUID();
        jdbc.update("INSERT INTO merchants (id, name, api_key_hash) VALUES (?,?,?)", id, req.name().trim(), SecurityConfig.sha256(apiKey));
        return Map.of("merchantId", id, "name", req.name().trim(), "apiKey", apiKey);
    }

    @GetMapping("/transactions")
    @Tag(name = "Transactions")
    public Map<String, Object> transactions(@AuthenticationPrincipal SecurityConfig.MerchantPrincipal m, @RequestParam(required = false) String type,
                                            @RequestParam(defaultValue = "0") int page, @RequestParam(defaultValue = "20") int size) {
        return payments.transactions(m.merchantId(), type, page, size);
    }

    @GetMapping("/ledger/journals/{journalId}")
    @Tag(name = "Ledger")
    public Map<String, Object> journal(@AuthenticationPrincipal SecurityConfig.MerchantPrincipal m, @PathVariable UUID journalId) {
        List<Map<String, Object>> j = jdbc.queryForList("SELECT id, reference_type, reference_id, reverses_journal_id, description, created_at FROM ledger_journals WHERE id = ? AND merchant_id = ?", journalId, m.merchantId());
        if (j.isEmpty()) {
            throw ApiException.notFound("journal");
        }
        List<Map<String, Object>> entries = jdbc.queryForList("SELECT e.id, a.account_type, e.debit_minor, e.credit_minor, e.currency FROM ledger_entries e JOIN ledger_accounts a ON a.id = e.account_id WHERE e.journal_id = ? ORDER BY e.debit_minor DESC, e.id", journalId);
        Map<String, Object> out = new java.util.LinkedHashMap<>(Camel.map(j.get(0)));
        out.put("entries", Camel.list(entries));
        return out;
    }

    /** Ledger-derived amount the platform owes this merchant (credits - debits on the merchant payable account). */
    @GetMapping("/ledger/balance")
    @Tag(name = "Ledger")
    public Map<String, Object> balance(@AuthenticationPrincipal SecurityConfig.MerchantPrincipal m) {
        return Map.of("merchantPayableMinor", ledger.merchantPayableBalance(m.merchantId()), "currency", "USD");
    }
}
