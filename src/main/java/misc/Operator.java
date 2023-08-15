package misc;

public enum Operator {

    SMALLER_THAN,
    SMALLER_OR_EQUAL_TO,
    GREATER_THAN,
    GREATER_OR_EQUAL_TO,
    EQUAL,
    NOT_EQUAL;

    public static <T extends Comparable<T>> boolean matches(T value1, T value2, Operator operator) {

        if (value1 == null || value2 == null) {
            return false; // You can decide how to handle null values
        }

        switch (operator) {
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
                throw new IllegalArgumentException("Unsupported operator: " + operator);
        }
    }
}
