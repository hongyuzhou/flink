package org.apache.flink.streaming.examples.windowing.util;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.source.datagen.RandomGenerator;
import org.apache.flink.util.AbstractID;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * <city, event_time, value>
 */
public class Tuple3SourceGenerator extends RandomGenerator<Tuple3<String, Long, String>> {

    private final List<String> city = Collections.unmodifiableList(Arrays.asList(
            "A", "B", "C", "D",
            "E", "F", "G", "H",
            "I", "J", "K", "L",
            "M", "N", "O", "P",
            "Q", "R", "S", "T",
            "U", "V", "W", "X"));

    @Override
    public Tuple3<String, Long, String> next() {
        AbstractID id = new AbstractID();

        return Tuple3.of(
                city.get(random.nextInt(0, city.size() - 1)),
                System.currentTimeMillis(),
                id.toHexString());
    }
}
