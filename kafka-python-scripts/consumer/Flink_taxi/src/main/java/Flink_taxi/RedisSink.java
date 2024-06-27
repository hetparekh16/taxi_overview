package Flink_taxi;

import Dto.TaxiAverageSpeed;
import Dto.TaxiDistance;
import Dto.TaxiSpeed;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import redis.clients.jedis.Jedis;

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
        }
    }
}
