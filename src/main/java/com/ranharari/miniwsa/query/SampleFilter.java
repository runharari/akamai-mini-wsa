package com.ranharari.miniwsa.query;

import com.ranharari.miniwsa.common.model.Action;
import com.ranharari.miniwsa.common.model.Category;

/** What the sample events must match. A null {@code configId}, {@code category} or {@code action} means any. */
public record SampleFilter(Long configId, TimeRange range, Category category, Action action) {
}
