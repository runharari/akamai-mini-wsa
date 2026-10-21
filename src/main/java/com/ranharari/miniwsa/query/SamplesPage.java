package com.ranharari.miniwsa.query;

import java.util.List;

/** The answer of {@code GET /v1/events/samples}. {@code total} counts all matches, not just this page. */
public record SamplesPage(long total, int limit, int offset, List<EventSample> events) {
}
