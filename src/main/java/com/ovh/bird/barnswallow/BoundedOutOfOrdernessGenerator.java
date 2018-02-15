package com.ovh.bird.kafka.prototype;

import javax.annotation.Nullable;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.streaming.api.watermark.Watermark;

public class BoundedOutOfOrdernessGenerator implements
        org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks<org.apache.flink.api.java.tuple.Tuple6<String, String, Long, String, Long, Double>>,
        org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks<org.apache.flink.api.java.tuple.Tuple6<String, String, Long, String, Long, Double>> {
    
    private final long maxOutOfOrderness = 1_000L; // 1 SECOND
    private long currentMaxTimestamp = 0;

    @Override
    public Watermark getCurrentWatermark() {
        return new Watermark(currentMaxTimestamp - maxOutOfOrderness);
    }
    
    
    @Override
    public long extractTimestamp(Tuple6<String, String, Long, String, Long, Double> tuple6, long l) {
        long timestamp = tuple6.f2;     // Get processing timestamp
        currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp);
        return timestamp;
    }
    
    
    
    @Nullable
    @Override
    public Watermark checkAndGetNextWatermark(Tuple6<String, String, Long, String, Long, Double> tuple6, long l) {
        return null;
    }
}
