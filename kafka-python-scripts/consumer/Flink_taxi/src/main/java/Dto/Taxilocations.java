package Dto;
import lombok.Data;

@Data
public class Taxilocations {
    private String taxi_id;
    private double longitude;
    private double latitude;
    
    private String timestamp;

    public Taxilocations() {}

    public Taxilocations(String taxi_id, double longitude , double latitude,  String timestamp) {
        this.taxi_id = taxi_id;
        this.longitude = longitude;
        this.latitude = latitude;
        
        this.timestamp = timestamp;
    }

    public String getTaxi_id() {
        return taxi_id;
    }

        public double getLongitude() {
        return longitude;
    }

    public double getLatitude() {
        return latitude;
    }

    public String getTimestamp() {
        return timestamp;
    }
}
