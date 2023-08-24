package models.control;

import java.io.Serializable;
import java.util.Date;

public class ControlMessage implements Serializable {

    private String targetField;

    private Comparison comparison;

    private Long customerId;

    private Long alertId;

    private Date createdAt;

    public void setTargetField(String targetField) {
        this.targetField = targetField;
    }

    public void setComparison(Comparison comparison) {
        this.comparison = comparison;
    }

    public void setCustomerId(Long customerId) {
        this.customerId = customerId;
    }

    public void setAlertId(Long alertId) {
        this.alertId = alertId;
    }

    public void setCreatedAt(Date createdAt) {
        this.createdAt = createdAt;
    }

    public String getTargetField() {
        return targetField;
    }

    public Comparison getComparison() {
        return comparison;
    }

    public Long getCustomerId() {
        return customerId;
    }

    public Long getAlertId() {
        return alertId;
    }

    public Date getCreatedAt() {
        return createdAt;
    }

    @Override
    public String toString() {
        return "ControlMessage{" +
                "targetField='" + targetField + '\'' +
                ", comparison=" + comparison +
                ", customerId=" + customerId +
                ", alertId=" + alertId +
                ", createdAt=" + createdAt +
                '}';
    }
}
