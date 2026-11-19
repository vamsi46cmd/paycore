package io.paycore;

import jakarta.servlet.http.HttpServletRequest;
import java.time.Instant;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.dao.QueryTimeoutException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.transaction.CannotCreateTransactionException;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.MissingRequestHeaderException;
import org.springframework.web.bind.MissingServletRequestParameterException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.method.annotation.MethodArgumentTypeMismatchException;

/** One error schema for every endpoint. Internals (SQL, stack traces) are never returned to the client. */
@RestControllerAdvice
public class ApiExceptionHandler {
    private static final Logger log = LoggerFactory.getLogger(ApiExceptionHandler.class);

    public record ApiError(Instant timestamp, int status, String code, String message, String path, List<String> details) {}

    static ResponseEntity<ApiError> resp(HttpStatus s, String code, String m, String path, List<String> d) {
        return ResponseEntity.status(s).body(new ApiError(Instant.now(), s.value(), code, m, path, d));
    }

    @ExceptionHandler(ApiException.class)
    ResponseEntity<ApiError> api(ApiException e, HttpServletRequest r) {
        return resp(e.status, e.code, e.getMessage(), r.getRequestURI(), null);
    }

    @ExceptionHandler(MethodArgumentNotValidException.class)
    ResponseEntity<ApiError> invalid(MethodArgumentNotValidException e, HttpServletRequest r) {
        List<String> d = e.getBindingResult().getFieldErrors().stream().map(f -> f.getField() + ": " + f.getDefaultMessage()).toList();
        return resp(HttpStatus.BAD_REQUEST, "validation_failed", "Validation failed", r.getRequestURI(), d);
    }

    @ExceptionHandler(MissingRequestHeaderException.class)
    ResponseEntity<ApiError> header(MissingRequestHeaderException e, HttpServletRequest r) {
        return resp(HttpStatus.BAD_REQUEST, "missing_header", "Required header missing: " + e.getHeaderName(), r.getRequestURI(), null);
    }

    @ExceptionHandler({HttpMessageNotReadableException.class, MethodArgumentTypeMismatchException.class,
            MissingServletRequestParameterException.class, IllegalArgumentException.class})
    ResponseEntity<ApiError> malformed(Exception e, HttpServletRequest r) {
        String m = e instanceof IllegalArgumentException ? e.getMessage() : "Malformed request";
        return resp(HttpStatus.BAD_REQUEST, "invalid_request", m, r.getRequestURI(), null);
    }

    @ExceptionHandler({DataAccessResourceFailureException.class, CannotCreateTransactionException.class, QueryTimeoutException.class})
    ResponseEntity<ApiError> dbDown(Exception e, HttpServletRequest r) {
        log.warn("Database unavailable on {} {}: {}", r.getMethod(), r.getRequestURI(), e.getClass().getSimpleName());
        return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).header("Retry-After", "5")
                .body(new ApiError(Instant.now(), 503, "service_unavailable", "Service temporarily unavailable; retry with the same Idempotency-Key", r.getRequestURI(), null));
    }

    @ExceptionHandler(Exception.class)
    ResponseEntity<ApiError> other(Exception e, HttpServletRequest r) {
        log.error("Unhandled error on {} {}", r.getMethod(), r.getRequestURI(), e);
        return resp(HttpStatus.INTERNAL_SERVER_ERROR, "internal_error", "Internal error", r.getRequestURI(), null);
    }
}
