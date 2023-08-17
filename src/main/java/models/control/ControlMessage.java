package models;

import java.io.Serializable;
import java.util.Date;

public class ControlMessage implements Serializable {

    private final String targetField;

    private final Comparison comparison;

    private final Long customerId;

    private final Long alertId;

    private final Date createdAt;

    public ControlMessage(String targetField, Comparison comparison, Long customerId, Long alertId) {

        this.targetField = targetField;
        this.comparison = comparison;
        this.customerId = customerId;
        this.alertId = alertId;
        createdAt = new Date();
    }
}
