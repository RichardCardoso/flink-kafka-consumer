package models;

import models.control.ControlMessage;

import java.util.List;

public class NotificationCandidate {

    private List<LiveMessage> liveMessages;
    private ControlMessage controlMessage;
    private String startTime;
    private String endTime;

    public NotificationCandidate() {
    }

    public NotificationCandidate(List<LiveMessage> liveMessages, ControlMessage controlMessage, String startTime, String endTime) {

        this.liveMessages = liveMessages;
        this.controlMessage = controlMessage;
        this.startTime = startTime;
        this.endTime = endTime;
    }

    public ControlMessage getControlMessage() {
        return controlMessage;
    }

    public void setControlMessage(ControlMessage controlMessage) {
        this.controlMessage = controlMessage;
    }

    public List<LiveMessage> getLiveMessages() {
        return liveMessages;
    }

    public void setLiveMessages(List<LiveMessage> liveMessages) {
        this.liveMessages = liveMessages;
    }

    public String getStartTime() {
        return startTime;
    }

    public void setStartTime(String startTime) {
        this.startTime = startTime;
    }

    public String getEndTime() {
        return endTime;
    }

    public void setEndTime(String endTime) {
        this.endTime = endTime;
    }

}
