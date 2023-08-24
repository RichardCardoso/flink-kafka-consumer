package models.control;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

public class ComparisonRule {

    private final static Logger log = LoggerFactory.getLogger(ComparisonRule.class);
    private static final String NULL_REF_VALUE_ERROR = "Reference value must be non null!";

    private Comparator comparator;
    private Double value1;
    private Double value2;
    private ComparisonType comparisonType;

    public boolean matches(Double reference) {

        if (Objects.isNull(reference)) {
            throw new RuntimeException(NULL_REF_VALUE_ERROR);
        }

        switch (comparisonType){
            case UNARY:
                return unaryMatch(reference);
            case BINARY:
                return binaryMatch(reference);
            default:
                throw new RuntimeException("Invalid comparison parameters");
        }
    }

    private boolean unaryMatch(Double reference) {

        log.info("Binary match test -> value1 {}, reference {}, comparator {}", value1, reference, comparator);
        if (value1 == null || reference == null) {
            return false; // You can decide how to handle null values
        }

        switch (comparator) {
            case SMALLER_THAN:
                return value1.compareTo(reference) < 0;
            case SMALLER_OR_EQUAL_TO:
                return value1.compareTo(reference) <= 0;
            case GREATER_THAN:
                return value1.compareTo(reference) > 0;
            case GREATER_OR_EQUAL_TO:
                return value1.compareTo(reference) >= 0;
            case EQUAL:
                return value1.compareTo(reference) == 0;
            case NOT_EQUAL:
                return value1.compareTo(reference) != 0;
            default:
                throw new IllegalArgumentException("Unsupported operator: " + comparator);
        }
    }

    private boolean binaryMatch(Double reference) {

        boolean ret;
        if (value1 == null || value2 == null || reference == null) {
            ret = false; // If any input is null, return false
        } else {
            switch (comparator) {
                case BETWEEN:
                    ret = value1.compareTo(reference) <= 0 && value2.compareTo(reference) >= 0;
                    break;
                case NOT_BETWEEN:
                    ret = value1.compareTo(reference) >= 0 || value2.compareTo(reference) <= 0;
                    break;
                default:
                    throw new IllegalArgumentException("Unsupported BinaryComparator: " + comparator);
            }
        }
        return ret;
    }

    @Override
    public String toString() {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("ComparisonRule [comparisonType=").append(comparisonType);

        switch (comparisonType) {
            case UNARY:
                stringBuilder.append(", comparator=").append(comparator);
                stringBuilder.append(", value1=").append(value1);
                break;
            case BINARY:
                stringBuilder.append(", comparator=").append(comparator);
                stringBuilder.append(", value1=").append(value1);
                stringBuilder.append(", value2=").append(value2);
                break;
        }

        stringBuilder.append("]");
        return stringBuilder.toString();
    }

    private enum ComparisonType {

        UNARY,
        BINARY
    }

    public Comparator getComparator() {
        return comparator;
    }

    public void setComparator(Comparator comparator) {
        this.comparator = comparator;
    }

    public Double getValue1() {
        return value1;
    }

    public void setValue1(Double value1) {
        this.value1 = value1;
    }

    public Double getValue2() {
        return value2;
    }

    public void setValue2(Double value2) {
        this.value2 = value2;
    }

    public ComparisonType getComparisonType() {
        return comparisonType;
    }

    public void setComparisonType(ComparisonType comparisonType) {
        this.comparisonType = comparisonType;
    }
}
