package com.ranharari.miniwsa.alerting;

import com.ranharari.miniwsa.common.model.Category;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.PositiveOrZero;

/** The body of {@code POST /v1/alerts/define}. The window is at most a day: that's how long counts are kept. */
public record AlertRuleRequest(
        String name,
        @NotNull Category category,
        @PositiveOrZero Long configId,
        @NotNull @PositiveOrZero Long threshold,
        @NotNull @Min(1) @Max(CountKey.MAX_WINDOW_MINUTES) Integer windowMinutes) {
}
