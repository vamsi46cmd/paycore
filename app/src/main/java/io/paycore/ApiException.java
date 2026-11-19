package io.paycore;

import org.springframework.http.HttpStatus;

public class ApiException extends RuntimeException {
    public final HttpStatus status;
    public final String code;

    public ApiException(HttpStatus status, String code, String message) {
        super(message);
        this.status = status;
        this.code = code;
    }

    public static ApiException notFound(String what) { return new ApiException(HttpStatus.NOT_FOUND, "not_found", what + " not found"); }

    public static ApiException conflict(String code, String m) { return new ApiException(HttpStatus.CONFLICT, code, m); }

    public static ApiException badRequest(String m) { return new ApiException(HttpStatus.BAD_REQUEST, "invalid_request", m); }

    public static ApiException unprocessable(String code, String m) { return new ApiException(HttpStatus.UNPROCESSABLE_ENTITY, code, m); }
}
