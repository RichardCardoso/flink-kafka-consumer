package models;

import misc.Operator;

import java.io.Serializable;

public class ControlMessage implements Serializable {

    private String targetField;

    private Operator operator;
    private Long value;

    private double thresholdPercent;

    private Long windowSize;
    private Long slideSize;

    private Long customerId;

    private Long alertId;

    public ControlMessage() {
    }

    public ControlMessage(String targetField, Operator operator, Long value, double thresholdPercent, Long windowSize, Long slideSize, Long customerId, Long alertId) {
        this.targetField = targetField;
        this.operator = operator;
        this.value = value;
        this.thresholdPercent = thresholdPercent;
        this.windowSize = windowSize;
        this.slideSize = slideSize;
        this.customerId = customerId;
        this.alertId = alertId;
    }

    public Operator getOperator() {
        return operator;
    }

    public void setOperator(Operator operator) {
        this.operator = operator;
    }

    public Long getValue() {
        return value;
    }

    public void setValue(Long value) {
        this.value = value;
    }

    public Long getWindowSize() {
        return windowSize;
    }

    public void setWindowSize(Long windowSize) {
        this.windowSize = windowSize;
    }

    public Long getSlideSize() {
        return slideSize;
    }

    public void setSlideSize(Long slideSize) {
        this.slideSize = slideSize;
    }

    public Long getAlertId() {
        return alertId;
    }

    public void setAlertId(Long alertId) {
        this.alertId = alertId;
    }

    public double getThresholdPercent() {
        return thresholdPercent;
    }

    public void setThresholdPercent(double thresholdPercent) {
        this.thresholdPercent = thresholdPercent;
    }

    public String getTargetField() {
        return targetField;
    }

    public void setTargetField(String targetField) {
        this.targetField = targetField;
    }

    public Long getCustomerId() {
        return customerId;
    }

    public void setCustomerId(Long customerId) {
        this.customerId = customerId;
    }

    @Override
    public String toString() {
        return "ControlMessage{" +
                "targetField='" + targetField + '\'' +
                ", operator=" + operator +
                ", value=" + value +
                ", thresholdPercent=" + thresholdPercent +
                '}';
    }
}
