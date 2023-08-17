package models;

import java.util.Objects;

public class ComparisonRule<T extends Comparable<T>> {

    private final static String INVALID_UNARY_MSG_SUFFIX = " is not a valid unary comparator";
    private final static String INVALID_BINARY_MSG_SUFFIX = " is not a valid binary comparator";
    private static final String NULL_VALUES_ERROR = "Comparison between null values is not allowed!";
    private static final String NULL_REF_VALUE_ERROR = "Reference value must be non null!";

    private final Comparator comparator;
    private final T value1;
    private final T value2;
    private final T reference;
    private final ComparisonType comparisonType;

    public ComparisonRule(Comparator unaryComparator, T value1, T value2) {

        validateUnaryComparator(unaryComparator);
        validateUnaryValues(value1, value2);
        this.comparator = unaryComparator;
        this.value1 = value1;
        this.value2 = value2;
        this.reference = null;
        this.comparisonType = ComparisonType.UNARY;
    }

    public ComparisonRule(Comparator binaryComparator, T value1, T value2, T reference) {

        validateBinaryComparator(binaryComparator);
        validateBinaryValues(value1, value2, reference);
        this.comparator = binaryComparator;
        this.value1 = value1;
        this.value2 = value2;
        this.reference = reference;
        this.comparisonType = ComparisonType.BINARY;
    }

    public boolean match() {

        switch (comparisonType){
            case UNARY:
                return unaryMatch();
            case BINARY:
                return binaryMatch();
            default:
                throw new RuntimeException("Invalid comparison parameters");
        }
    }

    private <T extends Comparable<T>> boolean unaryMatch() {

        if (value1 == null || value2 == null) {
            return false; // You can decide how to handle null values
        }

        switch (comparator) {
            case SMALLER_THAN:
                return value1.compareTo(value2) < 0;
            case SMALLER_OR_EQUAL_TO:
                return value1.compareTo(value2) <= 0;
            case GREATER_THAN:
                return value1.compareTo(value2) > 0;
            case GREATER_OR_EQUAL_TO:
                return value1.compareTo(value2) >= 0;
            case EQUAL:
                return value1.compareTo(value2) == 0;
            case NOT_EQUAL:
                return value1.compareTo(value2) != 0;
            default:
                throw new IllegalArgumentException("Unsupported operator: " + comparator);
        }
    }

    private < T extends Comparable<T>> boolean binaryMatch() {

        if (value1 == null || value2 == null || reference == null) {
            return false; // If any input is null, return false
        }

        switch (comparator) {
            case BETWEEN:
                return value1.compareTo(reference) >= 0 && value2.compareTo(reference) <= 0;
            case NOT_BETWEEN:
                return value1.compareTo(reference) < 0 || value2.compareTo(reference) > 0;
            default:
                throw new IllegalArgumentException("Unsupported BinaryComparator: " + comparator);
        }
    }

    private static void validateUnaryComparator(Comparator comparator) {

        switch (comparator) {

            case BETWEEN:
            case NOT_BETWEEN:
                throw new RuntimeException(comparator + INVALID_UNARY_MSG_SUFFIX);
        }
    }

    private static void validateBinaryComparator(Comparator comparator) {

        try {
            validateUnaryComparator(comparator);
        } catch (Exception ex) {
            if (!ex.getMessage().endsWith(INVALID_UNARY_MSG_SUFFIX)) {
                throw ex;
            }
            return;
        }
        throw new RuntimeException(comparator + INVALID_BINARY_MSG_SUFFIX);
    }

    private static void validateUnaryValues(Object value1, Object value2) {

        if (Objects.isNull(value1) || Objects.isNull(value2)) {
            throw new RuntimeException(NULL_VALUES_ERROR);
        }
    }

    private void validateBinaryValues(T value1, T value2, T reference) {

        validateUnaryValues(value1, value2);
        if (Objects.isNull(reference)) {
            throw new RuntimeException(NULL_REF_VALUE_ERROR);
        }
    }

    @Override
    public String toString() {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("ComparisonRule [comparisonType=").append(comparisonType);

        switch (comparisonType) {
            case UNARY:
                stringBuilder.append(", comparator=").append(comparator);
                stringBuilder.append(", value1=").append(value1);
                stringBuilder.append(", value2=").append(value2);
                break;
            case BINARY:
                stringBuilder.append(", comparator=").append(comparator);
                stringBuilder.append(", value1=").append(value1);
                stringBuilder.append(", value2=").append(value2);
                stringBuilder.append(", reference=").append(reference);
                break;
        }

        stringBuilder.append("]");
        return stringBuilder.toString();
    }

    private enum ComparisonType {

        UNARY,
        BINARY;
    }
}
