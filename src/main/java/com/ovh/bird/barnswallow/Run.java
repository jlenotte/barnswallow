package com.ovh.bird.kafka.prototype;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Cluster.Builder;
import java.text.Format;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.cassandra.CassandraTupleSink;
import org.apache.flink.streaming.connectors.cassandra.ClusterBuilder;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer08;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.util.Collector;
import org.joda.time.DateTimeZone;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Prototype of padda-stream processing
 * TODO :
 * - Manage DataSource
 * - Manage Static Data
 * - Manage Processes
 * - Manage DataSinks
 */
public class Run {
    
    // Logger
    private static final Logger LOG = LoggerFactory.getLogger(com.ovh.bird.woodpecker.flink.Run.class);
    
    // Timezone
    public static final DateTimeZone UTC = DateTimeZone.forID("UTC");
    
    // Properties
    private static final String PROPERTIES = "padda-stream.properties";
    
    private static final Integer FIELD_MESSAGE_ID = 0;
    private static final Integer FIELD_DEVICE_ID = 1;
    private static final Integer FIELD_TIMESTAMP = 2;
    private static final Integer FIELD_CATEGORY = 3;
    private static final Integer FIELD_MEASURE1 = 4;
    private static final int FIELD_MEASURE2 = 5;
    private static final String VERSION = "xxxxxxx";
    
    
    
    /**
     * main
     *
     * @param args String[]
     */
    public static void main(String[] args) throws Exception {
        
        // Time characteristics
        String timeCharacteristics = "EventTime";
        if (args.length > 0) {
            timeCharacteristics = args[0];
        }
        
        // Manage the port
        final int port;
        
        try {
            final ParameterTool config = ParameterTool.fromArgs(args);
            port = config.getInt("port");
        }
        catch (Exception e) {
            LOG.error("No port specified. Please run 'padda-stream --port <port>'");
            return;
        }
        
        // Manage execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000); // checkpoint every 5sec
        env.setParallelism(4);
        
        if ("EventTime".equals(timeCharacteristics)) {
            env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        }
        else if ("ProcessingTime".equals(timeCharacteristics)) {
            env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        }
        else if ("IngestionTime".equals(timeCharacteristics)) {
            env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);
        }
        
        final Format windowTimeFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        
        // Kafka properties
        Properties kafkaConfig = new Properties();
        kafkaConfig.setProperty("bootstrap.servers", "ks1:9092, ks2:9092, ks3:9092");
        kafkaConfig.setProperty("zookeeper.connect", "zk1:2181");
        kafkaConfig.setProperty("group.id", "flinkGroup");
        
        // Gather data from Kafka, parse, assign time and watermarks
        DataStream<Tuple6<String, String, Long, String, Long, Double>> parsedAndTimedStream = env
                .addSource(new FlinkKafkaConsumer08<>(
                        "sampletopic",
                        new SimpleStringSchema(),
                        kafkaConfig
                ))
                .rebalance()
                .map(new MapFunction<String, Tuple6<String, String, Long, String, Long, Double>>() {
                    
                    private static final long serialVersionUID = 34_2016_10_19_001L;
                    
                    
                    
                    @Override
                    public Tuple6<String, String, Long, String, Long, Double> map(String s)
                            throws Exception {
                        String[] splits = s.split("\\|");
                        return new Tuple6<String, String, Long, String, Long, Double>(
                                
                                splits[FIELD_MESSAGE_ID],
                                splits[FIELD_DEVICE_ID],
                                Long.parseLong(splits[FIELD_TIMESTAMP]),
                                splits[FIELD_CATEGORY],
                                Long.parseLong(splits[FIELD_MEASURE1]),
                                Double.parseDouble(splits[FIELD_MEASURE2])
                        );
                    }
                })
                .assignTimestampsAndWatermarks((AssignerWithPeriodicWatermarks<Tuple6<String, String, Long, String, Long, Double>>) new BoundedOutOfOrdernessGenerator());
        
        // Deduplicate on message ID
        WindowedStream<Tuple6<String, String, Long, String, Long, Double>, Tuple, TimeWindow> windowedStreamDeduplication = parsedAndTimedStream
                .keyBy(FIELD_MESSAGE_ID)
                .timeWindow(Time.of(5000, TimeUnit.MILLISECONDS),
                        Time.of(5000, TimeUnit.MILLISECONDS));
        
        DataStream<Tuple6<String, String, Long, String, Long, Double>> deduplicatedStream = windowedStreamDeduplication
                .apply(new WindowFunction<Tuple6<String, String, Long, String, Long, Double>, Tuple6<String, String, Long, String, Long, Double>, Tuple, TimeWindow>() {
                    
                    @Override
                    public void apply(Tuple tuple,
                            TimeWindow window,
                            Iterable<Tuple6<String, String, Long, String, Long, Double>> iterable,
                            Collector<Tuple6<String, String, Long, String, Long, Double>> collector)
                            throws Exception {
                        collector.collect(iterable.iterator().next());
                    }
                });
        
        // Group by device ID, Category
        WindowedStream<Tuple6<String, String, Long, String, Long, Double>, Tuple, TimeWindow> windowedStreamGroupBy = deduplicatedStream
                .keyBy(FIELD_DEVICE_ID, FIELD_CATEGORY)
                .timeWindow(Time.of(5000, TimeUnit.MILLISECONDS),
                        Time.of(5000, TimeUnit.MILLISECONDS));
        
        // Debug information on windowedStreamGroupBy
        windowedStreamGroupBy
                .apply(new WindowFunction<Tuple6<String, String, Long, String, Long, Double>, Tuple2<String, String>, Tuple, TimeWindow>() {
                    @Override
                    public void apply(Tuple tuple,
                            TimeWindow window,
                            Iterable<Tuple6<String, String, Long, String, Long, Double>> iterable,
                            Collector<Tuple2<String, String>> collector) throws Exception {

                        for (Tuple6<String, String, Long, String, Long, Double> value : iterable) {

                            collector.collect(new Tuple2<String, String>(
                                    "v" + VERSION + "- windowedStreamGroupBy -" + Instant.now()
                                            .toString(),
                                    "MESSAGE_ID=" + value.getField(0).toString() + ", "
                                            + "DEVICE_ID=" + value.getField(1).toString() + ", "
                                            + "TIMESTAMP=" + value.getField(2).toString() + ", "
                                            + "Time window start="
                                            + (new Long(window.getStart()).toString()) + ", "
                                            + "Time window end="
                                            + (new Long(window.getEnd()).toString()) + ", "
                                            + "CATEGORY=" + value.getField(3).toString() + ", "
                                            + "M1=" + value.getField(4).toString() + ", "
                                            + "M2=" + value.getField(5).toString()
                            ));
                        }
                    }
                })
                .addSink(new CassandraTupleSink<Tuple2<String, String>>(
                        "INSERT INTO mydata.debug"
                                + " (id, message)"
                                + " VALUES (?, ?):",
                        new ClusterBuilder() {
                            @Override
                            protected Cluster buildCluster(Builder builder) {
                                return builder
                                        .addContactPoint("cassandra1").withPort(9042)
                                        .addContactPoint("cassandra2").withPort(9042)
                                        .addContactPoint("cassandra3").withPort(9042)
                                        .build();
                            }
                        }
                ));
        
        // Calculate sums for M1 & M2
        DataStream<Tuple5<String, String, String, Long, Double>> aggregatedStream = windowedStreamGroupBy
                .apply(new WindowFunction<Tuple6<String, String, Long, String, Long, Double>, Tuple5<String, String, String, Long, Double>, Tuple, TimeWindow>() {
                    
                    @Override
                    public void apply(Tuple tuple, TimeWindow window,
                            Iterable<Tuple6<String, String, Long, String, Long, Double>> iterable,
                            Collector<Tuple5<String, String, String, Long, Double>> collector)
                            throws Exception {
                        long window_timestamp_ms = window.getEnd();
                        String device_id = tuple.getField(0);
                        String category = tuple.getField(1);
                        long sum_m1 = 0L;
                        Double sum_m2 = 0.0d;

                        for (Tuple6<String, String, Long, String, Long, Double> item : iterable) {
                            sum_m1 += item.f4;  // FIELD_MEASURE1
                            sum_m2 += item.f5;  // FIELD_MEASURE2

                        }
                        
                        collector.collect(new Tuple5<String, String, String, Long, Double>(
                                windowTimeFormat.format(new Date(window_timestamp_ms)),
                                device_id,
                                category,
                                sum_m1,
                                sum_m2
                        ));
                    }
                });
        
        
        // Send aggregations to destination
        aggregatedStream
                .addSink(new CassandraTupleSink<Tuple5<String, String, String, Long, Double>>(
                        "INSERT INTO mydata.agg_events (window_time, device_id, category, m1_sum_flink, m2_sum_flink)",
                        new ClusterBuilder() {
                            @Override
                            protected Cluster buildCluster(Builder builder) {
                                return builder
                                        .addContactPoint("cassandra1").withPort(9042)
                                        .addContactPoint("cassandra2").withPort(9042)
                                        .addContactPoint("cassandra3").withPort(9042)
                                        .build();
                            }
                        }
                ));
        
        /*
        // Get input Data by connecting to socket
        DataStream<String> data = env.socketTextStream("localhost", port, "\n");
        
        // Parse & process Data, group, window, aggregate, etc.
        WindowedStream<Tuple3<String, String, BillingTurnover>, Tuple, TimeWindow> windowing = data
                .flatMap(new BillingProcessor())
                .keyBy("0")
                .timeWindow(Time.seconds(5), Time.seconds(1));
        */
        
        env.execute("prototype-streaming" + VERSION + " (" + timeCharacteristics + ")");
    }
}
