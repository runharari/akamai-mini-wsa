package com.ranharari.miniwsa.common.web;

import jakarta.servlet.Filter;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.ServletRequest;
import jakarta.servlet.ServletResponse;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.slf4j.MDC;

import java.io.IOException;
import java.util.UUID;

/**
 * Gives every HTTP request an id (the client's X-Request-Id, or a new one), puts it in the log
 * context so each log line of the request shows it, and returns it in the response header.
 */
public class RequestIdFilter implements Filter {

    public static final String HEADER = "X-Request-Id";
    public static final String MDC_KEY = "requestId";
    /** A longer client-sent id is replaced, so a client can't flood the logs through it. */
    private static final int MAX_LENGTH = 64;

    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
            throws IOException, ServletException {
        String requestId = ((HttpServletRequest) request).getHeader(HEADER);
        if (requestId == null || requestId.isBlank() || requestId.length() > MAX_LENGTH) {
            requestId = UUID.randomUUID().toString();
        }
        MDC.put(MDC_KEY, requestId);
        ((HttpServletResponse) response).setHeader(HEADER, requestId);
        try {
            chain.doFilter(request, response);
        } finally {
            MDC.remove(MDC_KEY);
        }
    }
}
