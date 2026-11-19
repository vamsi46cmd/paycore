package io.paycore;

import io.swagger.v3.oas.annotations.tags.Tag;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

/** Operator endpoints (X-Admin-Key). Reconciliation covers all merchants. */
@RestController
@RequestMapping("/api/v1/reconciliation/runs")
@Tag(name = "Reconciliation")
@ConditionalOnProperty(name = "paycore.roles.api", havingValue = "true")
public class ReconciliationController {
    private final ReconciliationService service;

    public ReconciliationController(ReconciliationService service) {
        this.service = service;
    }

    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
    public Map<String, Object> start(@RequestParam(defaultValue = "false") boolean repair) {
        return service.run(service.run(repair));
    }

    @GetMapping
    public List<Map<String, Object>> list() {
        return service.runs();
    }

    @GetMapping("/{id}")
    public Map<String, Object> get(@PathVariable UUID id) {
        return service.run(id);
    }
}
