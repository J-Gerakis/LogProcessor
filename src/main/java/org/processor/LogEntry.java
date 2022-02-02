package org.processor;

import lombok.Getter;
import lombok.Setter;

/**
 * Data class for JSON log element
 */
@Getter
@Setter
public class LogEntry {

    private String id;
    private String state;
    private long timestamp = -1;
    private String host;
    private String type;

    @Override
    public String toString() {
        return "LogEntry{" +
                "id='" + id + '\'' +
                ", state='" + state + '\'' +
                ", timestamp=" + timestamp +
                ", host='" + host + '\'' +
                ", type='" + type + '\'' +
                '}';
    }
}
