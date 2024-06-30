package Flink_taxi;

import Dto.TaxiAverageSpeed;
import Dto.TaxiDistance;
import Dto.TaxiSpeed;
import Dto.Taxilocations;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import redis.clients.jedis.Jedis;
import org.json.JSONObject;

public class RedisSink<T> extends RichSinkFunction<T> {
    private transient Jedis jedis;

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
        if (value instanceof TaxiSpeed) {
            TaxiSpeed speed = (TaxiSpeed) value;
            jedis.hset("taxi:speed", speed.getTaxi_id(), String.valueOf(speed.getSpeed()));
        } else if (value instanceof TaxiAverageSpeed) {
            TaxiAverageSpeed avgSpeed = (TaxiAverageSpeed) value;
            jedis.hset("taxi:avgSpeed", avgSpeed.getTaxi_id(), String.valueOf(avgSpeed.getAverageSpeed()));
        } else if (value instanceof TaxiDistance) {
            TaxiDistance distance = (TaxiDistance) value;
            jedis.hset("taxi:distance", distance.getTaxi_id(), String.valueOf(distance.getDistance()));
        } else if (value instanceof Taxilocations) {
            Taxilocations location = (Taxilocations) value;
            // Create a JSON object for the location data
            JSONObject json = new JSONObject();
            json.put("latitude", location.getLatitude());
            json.put("longitude", location.getLongitude());
            json.put("timestamp", location.getTimestamp());

            // Convert the JSON object to a string
            String locationJson = json.toString();

            // Store the JSON string in Redis
            jedis.hset("taxi:location", location.getTaxi_id(), locationJson);
        } else if (value instanceof String) {
            String notification = (String) value;
            jedis.lpush("taxi:notifications", notification);
        }
    }
}