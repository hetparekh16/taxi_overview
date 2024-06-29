

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
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import Deserializer.JSONValueDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
//import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.util.Collector;
import java.lang.Math;
import java.text.ParseException;
import java.text.SimpleDateFormat;



public class StreamingJob {
	public static final double FORBIDDEN_CITY_LAT = 39.9163;
	public static final double FORBIDDEN_CITY_LON = 116.3972;
	public static final double INNER_RADIUS_KM = 10.0;
	public static final double OUTER_RADIUS_KM = 15.0;
	public static final double SPEED_LIMIT_KMH = 50.0;

	public static void main(String[] args) throws Exception {
		// set up the streaming execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		String topic = "taxi-locations";

		KafkaSource<Taxilocations> source = KafkaSource.<Taxilocations>builder()
				.setBootstrapServers("kafka:29092")
				.setTopics(topic)
				.setGroupId("flink-taxi")
				.setStartingOffsets(OffsetsInitializer.earliest())
				.setValueOnlyDeserializer(new JSONValueDeserializationSchema())
				.build();

		DataStream<Taxilocations> taxilocationsDataStream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka source");
//

//		Taxilocations.print("TaxiLocation Data");

		// Calculate Speed Operator
		DataStream<TaxiSpeed> taxiSpeeds = taxilocationsDataStream
				.keyBy(Taxilocations::getTaxi_id)
				.process(new CalculateSpeed());

		// Notify Speeding
		DataStream<String> speedingNotifications = taxiSpeeds
				.keyBy(TaxiSpeed::getTaxi_id)
				.process(new NotifySpeeding());

		speedingNotifications.addSink(new SinkFunction<String>() {
			@Override
			public void invoke(String value, Context context) {
				System.out.println(value);
			}
		});

		speedingNotifications.addSink(new RedisSink<>());

		// Notify Leaving Area
		DataStream<String> leavingAreaNotifications = taxilocationsDataStream
				.keyBy(Taxilocations::getTaxi_id)
				.process(new NotifyLeavingArea());

		leavingAreaNotifications.addSink(new SinkFunction<String>() {
			@Override
			public void invoke(String value, Context context) {
				System.out.println(value);
			}
		});
		leavingAreaNotifications.addSink(new RedisSink<>());

//		taxiSpeeds.print("TaxiSpeed Data");

		// Print Speed
		taxiSpeeds.addSink(new SinkFunction<>() {
			@Override
			public void invoke(TaxiSpeed value, Context context) {
				System.out.printf("Taxi ID %s - Speed: %.2f km/h\n", value.getTaxi_id(), value.getSpeed());
			}
		});
		// Calculate Average Speed Operator
		DataStream<TaxiAverageSpeed> averageSpeeds = taxiSpeeds
				.keyBy(TaxiSpeed::getTaxi_id)
				.process(new CalculateAverageSpeed());

		// Print Average Speed
		averageSpeeds.addSink(new SinkFunction<>() {
			@Override
			public void invoke(TaxiAverageSpeed value, Context context) {
				System.out.printf("Taxi ID %s - Avg Speed: %.2f km/h\n", value.getTaxi_id(), value.getAverageSpeed());
			}
		});

		averageSpeeds.print("TaxiAverageSpeed Data");

		// Calculate Distance Operator
		DataStream<TaxiDistance> distances = taxilocationsDataStream
				.keyBy(Taxilocations::getTaxi_id)
				.process(new CalculateDistance());

		// Print Distance
		distances.addSink(new SinkFunction<>() {
			@Override
			public void invoke(TaxiDistance value, Context context) {
				System.out.printf("Taxi ID %s - Distance: %.2f km\n", value.getTaxi_id(), value.getDistance());
			}
		});
		distances.print("TaxiDistance Data");

		taxilocationsDataStream.print();
		// Print results to console
//		taxiSpeeds.print();
//		averageSpeeds.print();
//		distances.print();
		// Add Redis Sinks
		taxilocationsDataStream.addSink(new RedisSink<>());
		taxiSpeeds.addSink(new RedisSink<>());
		averageSpeeds.addSink(new RedisSink<>());
		distances.addSink(new RedisSink<>());
		// execute program
		env.execute("Flink Streaming Java API Skeleton");
	}

	public static class NotifySpeeding extends KeyedProcessFunction<String, TaxiSpeed, String> {
		@Override
		public void processElement(TaxiSpeed speed, Context context, Collector<String> out) throws Exception {
			if (speed.getSpeed() > SPEED_LIMIT_KMH) {
				out.collect("Warning: Taxi ID " + speed.getTaxi_id() + " is speeding at " + speed.getSpeed() + " km/h.");
			}
		}
	}

	public static class NotifyLeavingArea extends KeyedProcessFunction<String, Taxilocations, String> {
		@Override
		public void processElement(Taxilocations location, Context context, Collector<String> out) throws Exception {
			double distanceFromCenter = Haversine.distance(FORBIDDEN_CITY_LAT, FORBIDDEN_CITY_LON, location.getLatitude(), location.getLongitude());
			if (distanceFromCenter > INNER_RADIUS_KM && distanceFromCenter <= OUTER_RADIUS_KM) {
				out.collect("Warning: Taxi ID " + location.getTaxi_id() + " is leaving the predefined area. Current distance: " + distanceFromCenter + " km.");
			} else if (distanceFromCenter > OUTER_RADIUS_KM) {
				// Discard information by not forwarding it
			}
		}
	}
	public static class CalculateSpeed extends KeyedProcessFunction<String, Taxilocations, TaxiSpeed> {
		private transient ValueState<Taxilocations> lastLocationState;
		private static final SimpleDateFormat timestampFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

		@Override
		public void open(Configuration parameters) throws Exception {
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

				double distance = Haversine.distance(
						lastLocation.getLatitude(), lastLocation.getLongitude(),
						currentLocation.getLatitude(), currentLocation.getLongitude()
				);
				double timeDiff = (currentTimestamp - lastTimestamp) / 3600000.0; // in hours
				double speed = distance / timeDiff; // in kilometers per hour

				out.collect(new TaxiSpeed(currentLocation.getTaxi_id(), speed));
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
		public void open(Configuration parameters) throws Exception {
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

			double averageSpeed = 0.0;
			if (currentState.f0 != 0) {
				averageSpeed = currentState.f1 / currentState.f0;
			}

			out.collect(new TaxiAverageSpeed(speed.getTaxi_id(), averageSpeed));

			speedState.update(currentState);
		}
	}

	public static class CalculateDistance extends KeyedProcessFunction<String, Taxilocations, TaxiDistance> {
		private transient ValueState<Double> distanceState;
		private transient ValueState<Taxilocations> lastLocationState;

		@Override
		public void open(Configuration parameters) throws Exception {
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

