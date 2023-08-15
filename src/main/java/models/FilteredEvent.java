package models;

public class FilteredEvent {

    private LiveMessage liveMessage;
    private ControlMessage controlMessage;

    public FilteredEvent(LiveMessage liveMessage, ControlMessage controlMessage) {
        this.liveMessage = liveMessage;
        this.controlMessage = controlMessage;
    }

    public LiveMessage getLiveMessage() {
        return liveMessage;
    }

    public void setLiveMessage(LiveMessage liveMessage) {
        this.liveMessage = liveMessage;
    }

    public ControlMessage getControlMessage() {
        return controlMessage;
    }

    public void setControlMessage(ControlMessage controlMessage) {
        this.controlMessage = controlMessage;
    }
}
