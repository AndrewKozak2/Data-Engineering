package com.example.lab4;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Component
public class TripProcessor {

    private static final ObjectMapper mapper = new ObjectMapper();

    @Autowired
    public void process(StreamsBuilder streamsBuilder) {
        KStream<String, String> trips = streamsBuilder.stream("Topic1", Consumed.with(Serdes.String(), Serdes.String()));

        KStream<String, String> tripsWithDateKey = trips.selectKey((key, value) -> {
            try {
                JsonNode node = mapper.readTree(value);
                return node.get("start_time").asText().split(" ")[0];
            } catch (Exception e) {
                return "unknown";
            }
        });

        // a. Avg duration & b. trip count
        tripsWithDateKey.groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
            .aggregate(
                () -> "0,0.0", // count,sum
                (key, value, aggregate) -> {
                    try {
                        String[] parts = aggregate.split(",");
                        long count = Long.parseLong(parts[0]) + 1;
                        JsonNode node = mapper.readTree(value);
                        String durStr = node.get("tripduration").asText().replace("\"", "").replace(",", "");
                        double sum = Double.parseDouble(parts[1]) + Double.parseDouble(durStr);
                        return count + "," + sum;
                    } catch (Exception e) { return aggregate; }
                },
                Materialized.with(Serdes.String(), Serdes.String())
            ).toStream()
            .mapValues(v -> {
                String[] parts = v.split(",");
                double count = Double.parseDouble(parts[0]);
                double avg = count > 0 ? Double.parseDouble(parts[1]) / count : 0;
                return "{\"count\":" + parts[0] + ", \"avg_duration\":" + avg + "}";
            })
            .peek((k, v) -> System.out.println("Stats for " + k + ": " + v))
            .to("trip-stats-daily", Produced.with(Serdes.String(), Serdes.String()));

        // c. Most popular start station
        tripsWithDateKey.groupBy((key, value) -> {
            try {
                JsonNode node = mapper.readTree(value);
                return key + "|" + node.get("from_station_name").asText();
            } catch (Exception e) { return key + "|unknown"; }
        }, Grouped.with(Serdes.String(), Serdes.String()))
            .count()
            .toStream()
            .map((key, count) -> {
                String[] parts = key.split("\\|");
                return KeyValue.pair(parts[0], parts[1] + ":" + count);
            })
            .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
            .aggregate(
                () -> "unknown:0",
                (key, value, aggregate) -> {
                    long currentCount = Long.parseLong(value.split(":")[1]);
                    long maxCount = Long.parseLong(aggregate.split(":")[1]);
                    return currentCount > maxCount ? value : aggregate;
                },
                Materialized.with(Serdes.String(), Serdes.String())
            ).toStream()
            .mapValues(v -> "{\"popular_start_station\":\"" + v.split(":")[0] + "\", \"count\":" + v.split(":")[1] + "}")
            .peek((k, v) -> System.out.println("Popular station for " + k + ": " + v))
            .to("popular-start-station", Produced.with(Serdes.String(), Serdes.String()));

        // d. Top 3 station leaders (start + finish)
        tripsWithDateKey.flatMap((key, value) -> {
            try {
                JsonNode node = mapper.readTree(value);
                String from = node.get("from_station_name").asText();
                String to = node.get("to_station_name").asText();
                return Arrays.asList(
                    KeyValue.pair(key + "|" + from, 1L),
                    KeyValue.pair(key + "|" + to, 1L)
                );
            } catch (Exception e) { return Collections.emptyList(); }
        })
        .groupByKey(Grouped.with(Serdes.String(), Serdes.Long()))
        .count()
        .toStream()
        .map((key, count) -> {
            String[] parts = key.split("\\|");
            return KeyValue.pair(parts[0], parts[1] + ":" + count);
        })
        .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
        .aggregate(
            () -> "",
            (key, value, aggregate) -> {
                Map<String, Long> stations = new HashMap<>();
                if (!aggregate.isEmpty()) {
                    for (String s : aggregate.split(";")) {
                        String[] p = s.split(":");
                        stations.put(p[0], Long.parseLong(p[1]));
                    }
                }
                String[] newVal = value.split(":");
                stations.put(newVal[0], Long.parseLong(newVal[1]));
                return stations.entrySet().stream()
                    .sorted(Map.Entry.<String, Long>comparingByValue().reversed())
                    .limit(3)
                    .map(e -> e.getKey() + ":" + e.getValue())
                    .collect(Collectors.joining(";"));
            },
            Materialized.with(Serdes.String(), Serdes.String())
        ).toStream()
        .mapValues(v -> "{\"top_stations\":\"" + v + "\"}")
        .peek((k, v) -> System.out.println("Top 3 for " + k + ": " + v))
        .to("top-stations", Produced.with(Serdes.String(), Serdes.String()));
    }
}
