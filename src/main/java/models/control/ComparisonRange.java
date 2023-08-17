package models;

import java.util.concurrent.TimeUnit;

public class ComparisonRange {

    private final RangeAggregationType aggregationType;
    private final Long length;
    private final Long slide;
    private final TimeUnit timeUnit;

    public static ComparisonRange of(RangeAggregationType aggregationType, Long length, Long slide, TimeUnit timeUnit) {

        return new ComparisonRange(aggregationType, length, slide, timeUnit);
    }

    private ComparisonRange(RangeAggregationType aggregationType, Long length, Long slide, TimeUnit timeUnit) {

        this.aggregationType = aggregationType;
        this.length = length;
        this.slide = slide;
        this.timeUnit = timeUnit;
    }

    public RangeAggregationType getAggregationType() {
        return aggregationType;
    }

    public Long getLength() {
        return length;
    }

    public Long getSlide() {
        return slide;
    }

    public TimeUnit getTimeUnit() {
        return timeUnit;
    }
}
