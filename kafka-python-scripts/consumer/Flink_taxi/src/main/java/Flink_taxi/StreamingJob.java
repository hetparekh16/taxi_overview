package Flink_taxi;

import Dto.Taxilocations;
import Dto.TaxiSpeed;
import Dto.TaxiAverageSpeed;
import Dto.TaxiDistance;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import Deserializer.JSONValueDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.text.SimpleDateFormat;

public class StreamingJob {

    private static final double FORBIDDEN_CITY_LAT = 39.9163;
    private static final double FORBIDDEN_CITY_LON = 116.3972;
    private static final double INNER_RADIUS_KM = 15.0;
    private static final double OUTER_RADIUS_KM = 30.0;
    private static final double SPEED_LIMIT_KMH = 50.0;
    private static final Logger LOG = LoggerFactory.getLogger(StreamingJob.class);

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        String kafkaBroker = "kafka:29092";
        String topic = "taxi-locations";

        KafkaSource<Taxilocations> source = KafkaSource.<Taxilocations>builder()
                .setBootstrapServers(kafkaBroker)
                .setTopics(topic)
                .setGroupId("flink-taxi")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new JSONValueDeserializationSchema())
                .build();

        DataStream<Taxilocations> taxilocationsDataStream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");

        DataStream<TaxiSpeed> taxiSpeeds = taxilocationsDataStream
                .keyBy(Taxilocations::getTaxi_id)
                .process(new CalculateSpeed());

        DataStream<String> speedingNotifications = taxiSpeeds
                .keyBy(TaxiSpeed::getTaxi_id)
                .process(new NotifySpeeding());

        speedingNotifications.addSink(createLoggingSink("Speeding Notification"));

        OutputTag<String> areaViolationTag = new OutputTag<String>("area_violation"){};
        OutputTag<String> leavingAreaWarningTag = new OutputTag<String>("leaving_area_warning_id"){};

        SingleOutputStreamOperator<String> leavingAreaNotifications = taxilocationsDataStream
                .keyBy(Taxilocations::getTaxi_id)
                .process(new NotifyLeavingArea(areaViolationTag));

        leavingAreaNotifications.addSink(createLoggingSink("Leaving Area Notification"));

        // Separate sinks for warnings and area violations
        leavingAreaNotifications.getSideOutput(leavingAreaWarningTag).addSink(new RedisSink<>("leavingAreaWarning"));
        leavingAreaNotifications.getSideOutput(areaViolationTag).addSink(new RedisSink<>("areaViolation"));


        taxiSpeeds.addSink(new RedisSink<>("speed"));
        DataStream<TaxiAverageSpeed> averageSpeeds = taxiSpeeds
                .keyBy(TaxiSpeed::getTaxi_id)
                .process(new CalculateAverageSpeed());

        averageSpeeds.addSink(new RedisSink<>("avgSpeed"));

        DataStream<TaxiDistance> distances = taxilocationsDataStream
                .keyBy(Taxilocations::getTaxi_id)
                .process(new CalculateDistance());

        distances.addSink(new RedisSink<>("distance"));

        taxilocationsDataStream.addSink(new RedisSink<>("location"));
        speedingNotifications.addSink(new RedisSink<>("notification"));

        env.execute("Flink Streaming Taxi Job");
    }

    private static SinkFunction<String> createLoggingSink(String label) {
        return new SinkFunction<String>() {
            @Override
            public void invoke(String value, Context context) {
                LOG.info("{}: {}", label, value);
            }
        };
    }

    public static class NotifySpeeding extends KeyedProcessFunction<String, TaxiSpeed, String> {
        @Override
        public void processElement(TaxiSpeed speed, Context context, Collector<String> out) {
            if (speed.getSpeed() > SPEED_LIMIT_KMH) {
                out.collect("Warning: Taxi ID " + speed.getTaxi_id() + " is speeding at " + speed.getSpeed() + " km/h.");
            }
        }
    }

    public static class NotifyLeavingArea extends KeyedProcessFunction<String, Taxilocations, String> {
        private final OutputTag<String> areaViolationTag;

        public NotifyLeavingArea(OutputTag<String> areaViolationTag) {
            this.areaViolationTag = areaViolationTag;
        }

        @Override
        public void processElement(Taxilocations location, Context context, Collector<String> out) {
            double distanceFromCenter = Haversine.distance(FORBIDDEN_CITY_LAT, FORBIDDEN_CITY_LON, location.getLatitude(), location.getLongitude());

            if (distanceFromCenter > INNER_RADIUS_KM && distanceFromCenter <= OUTER_RADIUS_KM) {
                String warningMessage = "Warning: Taxi ID " + location.getTaxi_id() + " is leaving the inner area. Current distance: " + distanceFromCenter + " km.";
                out.collect(warningMessage);
                // Output the taxi ID for warnings
                context.output(new OutputTag<String>("leaving_area_warning_id"){}, location.getTaxi_id());
            } else if (distanceFromCenter > OUTER_RADIUS_KM) {
                String violationMessage = "Violation: Taxi ID " + location.getTaxi_id() + " has left the outer area. Current distance: " + distanceFromCenter + " km.";
                context.output(areaViolationTag, violationMessage);
            }
        }
    }




    public static class CalculateSpeed extends KeyedProcessFunction<String, Taxilocations, TaxiSpeed> {
        private transient ValueState<Taxilocations> lastLocationState;
        private static final SimpleDateFormat timestampFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        @Override
        public void open(Configuration parameters) {
            ValueStateDescriptor<Taxilocations> descriptor =
                    new ValueStateDescriptor<>("lastLocation", TypeInformation.of(new TypeHint<Taxilocations>() {}));
            lastLocationState = getRuntimeContext().getState(descriptor);
        }

        @Override
        public void processElement(Taxilocations currentLocation, Context context, Collector<TaxiSpeed> out) throws Exception {
            Taxilocations lastLocation = lastLocationState.value();

            if (lastLocation != null) {
                long currentTimestamp = parseTimestamp(currentLocation.getTimestamp());
                long lastTimestamp = parseTimestamp(lastLocation.getTimestamp());

                if (currentTimestamp > lastTimestamp) {
                    double distance = Haversine.distance(
                            lastLocation.getLatitude(), lastLocation.getLongitude(),
                            currentLocation.getLatitude(), currentLocation.getLongitude()
                    );
                    double timeDiff = (currentTimestamp - lastTimestamp) / 3600000.0; // in hours

                    if (timeDiff > 0) {
                        double speed = distance / timeDiff; // in kilometers per hour
                        LOG.info("Taxi ID: {}, Distance: {}, TimeDiff: {}, Speed: {}",
                                currentLocation.getTaxi_id(), distance, timeDiff, speed);

                        if (speed >= 0 && speed <= 300) { // Assuming 300 km/h as a reasonable upper limit
                            out.collect(new TaxiSpeed(currentLocation.getTaxi_id(), speed));
                        } else {
                            LOG.warn("Unrealistic speed detected for Taxi ID {}: Speed: {}", currentLocation.getTaxi_id(), speed);
                        }
                    } else {
                        LOG.warn("Time difference is zero or negative for Taxi ID {}", currentLocation.getTaxi_id());
                    }
                } else {
                    LOG.warn("Current timestamp is not greater than last timestamp for Taxi ID {}: current={}, last={}",
                            currentLocation.getTaxi_id(), currentTimestamp, lastTimestamp);
                }
            }

            lastLocationState.update(currentLocation);
        }

        private long parseTimestamp(String timestamp) throws ParseException {
            return timestampFormat.parse(timestamp).getTime();
        }
    }

    public static class CalculateAverageSpeed extends KeyedProcessFunction<String, TaxiSpeed, TaxiAverageSpeed> {
        private transient ValueState<Tuple2<Integer, Double>> speedState;

        @Override
        public void open(Configuration parameters) {
            ValueStateDescriptor<Tuple2<Integer, Double>> descriptor =
                    new ValueStateDescriptor<>("speedState", TupleTypeInfo.getBasicTupleTypeInfo(Integer.class, Double.class));
            speedState = getRuntimeContext().getState(descriptor);
        }

        @Override
        public void processElement(TaxiSpeed speed, Context context, Collector<TaxiAverageSpeed> out) throws Exception {
            Tuple2<Integer, Double> currentState = speedState.value();

            if (currentState == null) {
                currentState = Tuple2.of(0, 0.0);
            }

            currentState.f0 += 1;
            currentState.f1 += speed.getSpeed();

            double averageSpeed = currentState.f0 != 0 ? currentState.f1 / currentState.f0 : 0.0;

            LOG.info("Taxi ID: {}, Count: {}, Total Speed: {}, Average Speed: {}",
                    speed.getTaxi_id(), currentState.f0, currentState.f1, averageSpeed);

            if (!Double.isNaN(averageSpeed) && averageSpeed >= 0) {
                out.collect(new TaxiAverageSpeed(speed.getTaxi_id(), averageSpeed));
            } else {
                LOG.warn("Calculated average speed is NaN or negative for taxi ID {}", speed.getTaxi_id());
            }

            speedState.update(currentState);
        }
    }

    public static class CalculateDistance extends KeyedProcessFunction<String, Taxilocations, TaxiDistance> {
        private transient ValueState<Double> distanceState;
        private transient ValueState<Taxilocations> lastLocationState;

        @Override
        public void open(Configuration parameters) {
            ValueStateDescriptor<Double> distanceDescriptor =
                    new ValueStateDescriptor<>("distanceState", Double.class);
            distanceState = getRuntimeContext().getState(distanceDescriptor);

            ValueStateDescriptor<Taxilocations> lastLocationDescriptor =
                    new ValueStateDescriptor<>("lastLocationState", Taxilocations.class);
            lastLocationState = getRuntimeContext().getState(lastLocationDescriptor);
        }

        @Override
        public void processElement(Taxilocations currentLocation, Context context, Collector<TaxiDistance> out) throws Exception {
            Double totalDistance = distanceState.value();
            Taxilocations lastLocation = lastLocationState.value();

            if (totalDistance == null) {
                totalDistance = 0.0;
            }

            if (lastLocation != null) {
                double distance = Haversine.distance(
                        lastLocation.getLatitude(), lastLocation.getLongitude(),
                        currentLocation.getLatitude(), currentLocation.getLongitude()
                );

                totalDistance += distance;
            }

            out.collect(new TaxiDistance(currentLocation.getTaxi_id(), totalDistance));

            distanceState.update(totalDistance);
            lastLocationState.update(currentLocation);
        }
    }
}
