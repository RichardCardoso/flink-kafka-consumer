package models;

public class LiveMessage {

    private double value;
    private String receivedTime;
    private Long wellId;

    public LiveMessage() {
    }

    public LiveMessage(double value, String receivedTime, Long wellId) {

        this.value = value;
        this.receivedTime = receivedTime;
        this.wellId = wellId;
    }

    public double getValue() {
        return value;
    }

    public void setValue(double value) {
        this.value = value;
    }

    public String getReceivedTime() {
        return receivedTime;
    }

    public void setReceivedTime(String receivedTime) {
        this.receivedTime = receivedTime;
    }

    public Long getWellId() {
        return wellId;
    }

    public void setWellId(Long wellId) {
        this.wellId = wellId;
    }

    @Override
    public String toString() {
        return "LiveMessage{" +
                "value=" + value +
                ", receivedTime='" + receivedTime + '\'' +
                ", wellId=" + wellId +
                '}';
    }
}
