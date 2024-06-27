package Dto;
import lombok.Data;

@Data
public class Taxilocations {
    private String taxi_id;
    private double latitude;
    private double longitude;
    private String timestamp;

    public Taxilocations() {}

    public Taxilocations(String taxi_id, double latitude, double longitude, String timestamp) {
        this.taxi_id = taxi_id;
        this.latitude = latitude;
        this.longitude = longitude;
        this.timestamp = timestamp;
    }

    public String getTaxi_id() {
        return taxi_id;
    }

    public double getLatitude() {
        return latitude;
    }

    public double getLongitude() {
        return longitude;
    }

    public String getTimestamp() {
        return timestamp;
    }
}
