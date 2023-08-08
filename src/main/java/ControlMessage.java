import java.io.Serializable;

public class ControlMessage implements Serializable {

    private String operator;
    private Long value;
    private Long windowSize;
    private Long slideSize;

    public ControlMessage(String operator, Long value, Long windowSize, Long slideSize) {
        this.operator = operator;
        this.value = value;
        this.windowSize = windowSize;
        this.slideSize = slideSize;
    }

    public String getOperator() {
        return operator;
    }

    public void setOperator(String operator) {
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
}
