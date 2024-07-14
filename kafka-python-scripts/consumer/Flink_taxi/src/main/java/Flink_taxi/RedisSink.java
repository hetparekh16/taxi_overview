package Flink_taxi;

import Dto.TaxiAverageSpeed;
import Dto.TaxiDistance;
import Dto.TaxiSpeed;
import Dto.Taxilocations;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import redis.clients.jedis.Jedis;

public class RedisSink<T> extends RichSinkFunction<T> {
    private transient Jedis jedis;
    private final String dataType;

    public RedisSink(String dataType) {
        this.dataType = dataType;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        jedis = new Jedis("redis", 6379); // Use the service name 'redis'
    }

    @Override
    public void close() throws Exception {
        if (jedis != null) {
            jedis.close();
        }
    }

    @Override
    public void invoke(T value, Context context) {
        switch (dataType) {
            case "speed":
                TaxiSpeed speed = (TaxiSpeed) value;
                jedis.hset("taxi:speed", speed.getTaxi_id(), String.valueOf(speed.getSpeed()));
                if (speed.getSpeed() > 50) { // Assuming 50 km/h as speed limit
                    jedis.sadd("taxi:speeding_ids", speed.getTaxi_id());
                    jedis.incr("taxi:speeding_incidents"); // Increment the speeding incidents counter
                }
                break;
            case "avgSpeed":
                TaxiAverageSpeed avgSpeed = (TaxiAverageSpeed) value;
                jedis.hset("taxi:avgSpeed", avgSpeed.getTaxi_id(), String.valueOf(avgSpeed.getAverageSpeed()));
                break;
            case "distance":
                TaxiDistance distance = (TaxiDistance) value;
                jedis.hset("taxi:distance", distance.getTaxi_id(), String.valueOf(distance.getDistance()));
                break;
            case "location":
                Taxilocations location = (Taxilocations) value;
                String locationKey = "taxi:location:" + location.getTaxi_id();
                jedis.hset(locationKey, "latitude", String.valueOf(location.getLatitude()));
                jedis.hset(locationKey, "longitude", String.valueOf(location.getLongitude()));
                jedis.hset(locationKey, "timestamp", location.getTimestamp());
                jedis.sadd("taxi:currently_driving", location.getTaxi_id());
                break;
            case "notification":
                String notification = (String) value;
                jedis.lpush("taxi:notifications", notification);
                break;
            case "areaViolation":
                String violationTaxiId = (String) value;
                jedis.sadd("taxi:area_violations_ids", violationTaxiId);
                jedis.incr("taxi:area_violations");  // Ensure area violations counter is updated
                break;
            case "leavingAreaWarning":
                String warningTaxiId = (String) value;
                jedis.sadd("taxi:leaving_area_warning_ids", warningTaxiId);
                jedis.incr("taxi:leaving_area_warnings");  // Ensure warning counter is updated
                break;
            default:
                throw new IllegalArgumentException("Unsupported data type: " + dataType);
        }
    }
}
