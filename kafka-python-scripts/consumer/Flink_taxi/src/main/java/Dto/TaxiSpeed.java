package Dto;

import lombok.Getter;

public class TaxiSpeed {
    private String taxi_id;
    @Getter
    private double speed;

    public TaxiSpeed(String taxi_id, double speed) {
        this.taxi_id = taxi_id;
        this.speed = speed;
    }
    public String getTaxi_id() {
        return taxi_id;
    }

}
