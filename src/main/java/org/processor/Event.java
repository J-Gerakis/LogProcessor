package org.processor;

/**
 * Data class for Event database element
 */
public class Event {
    private String id;
    private long duration;
    private String host;
    private String type;
    private boolean alert;

    public Event(String id, long duration, String host, String type, boolean alert) {
        this.id = id;
        this.duration = duration;
        this.host = host;
        this.type = type;
        this.alert = alert;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public long getDuration() {
        return duration;
    }

    public void setDuration(long duration) {
        this.duration = duration;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public boolean getAlert() {
        return alert;
    }

    public int getAlertAsInt() {
        if (alert) return 1;
        else return 0;
    }

    public void setAlert(boolean alert) {
        this.alert = alert;
    }

    @Override
    public String toString() {
        return "Event{" +
                "id='" + id + '\'' +
                ", duration=" + duration +
                ", host='" + host + '\'' +
                ", type='" + type + '\'' +
                ", alert=" + alert +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Event event = (Event) o;
        return duration == event.duration && alert == event.alert && id.equals(event.id);
    }

}
