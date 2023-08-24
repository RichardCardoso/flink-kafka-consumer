package models.control;

import java.util.concurrent.TimeUnit;

public class ComparisonRange {

    private RangeAggregationType aggregationType;
    private Long length;
    private Long slide;
    private TimeUnit timeUnit;

    public Long getLengthInMilliseconds() {

        return timeUnit.toMillis(length);
    }

    public Long getSlideInMilliseconds() {

        return timeUnit.toMillis(slide);
    }

    public RangeAggregationType getAggregationType() {
        return aggregationType;
    }

    public void setAggregationType(RangeAggregationType aggregationType) {
        this.aggregationType = aggregationType;
    }

    public Long getLength() {
        return length;
    }

    public void setLength(Long length) {
        this.length = length;
    }

    public Long getSlide() {
        return slide;
    }

    public void setSlide(Long slide) {
        this.slide = slide;
    }

    public TimeUnit getTimeUnit() {
        return timeUnit;
    }

    public void setTimeUnit(TimeUnit timeUnit) {
        this.timeUnit = timeUnit;
    }

    @Override
    public String toString() {
        return "ComparisonRange{" +
                "aggregationType=" + aggregationType +
                ", length=" + length +
                ", slide=" + slide +
                ", timeUnit=" + timeUnit +
                '}';
    }
}
