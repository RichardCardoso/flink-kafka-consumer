package models;

import models.control.ControlMessage;

public class WindowElement {

    private LiveMessage liveMessage;
    private ControlMessage controlMessage;

    public WindowElement(LiveMessage liveMessage, ControlMessage controlMessage) {
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
