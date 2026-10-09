package com.example.streamforge.alerts.kafka;

import com.example.streamforge.alerts.domain.TripEvent;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Map;

@JsonIgnoreProperties(ignoreUnknown = true)
public class Envelope {
    
    @JsonProperty("schema_version")
    private String schemaVersion;
    
    @JsonProperty("kind")
    private String kind;
    
    @JsonProperty("trip")
    private TripEvent trip;
    
    @JsonProperty("rejection")
    private Map<String, Object> rejection;

    public String getSchemaVersion() { return schemaVersion; }
    public void setSchemaVersion(String schemaVersion) { this.schemaVersion = schemaVersion; }
    public String getKind() { return kind; }
    public void setKind(String kind) { this.kind = kind; }
    public TripEvent getTrip() { return trip; }
    public void setTrip(TripEvent trip) { this.trip = trip; }
    public Map<String, Object> getRejection() { return rejection; }
    public void setRejection(Map<String, Object> rejection) { this.rejection = rejection; }
}
