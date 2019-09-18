package zipkin.storage.kafka.experiment;

import java.time.Duration;
import java.util.function.Supplier;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.TimeWindows;
import zipkin2.storage.kafka.streams.serdes.SpansSerde;

import static org.apache.kafka.streams.kstream.Suppressed.BufferConfig.unbounded;
import static org.apache.kafka.streams.kstream.Suppressed.untilWindowCloses;

public class PathAggregateTopology implements Supplier<Topology> {

  final String traceTopicName;
  final String pathTopicName;

  final SpansSerde spansSerde;
  final PathSerde pathSerde;
  final PathAggregateSerde pathAggregateSerde;

  PathAggregateTopology(PathAggregateProcessor processor) {
    this.traceTopicName = processor.traceTopicName;
    this.pathTopicName = processor.pathTopicName;
    spansSerde = new SpansSerde();
    pathSerde = new PathSerde();
    pathAggregateSerde = new PathAggregateSerde();
  }

  public Topology get() {
    StreamsBuilder builder = new StreamsBuilder();
    builder.stream(traceTopicName, Consumed.with(Serdes.String(), spansSerde))
        .mapValues((readOnlyKey, value) -> Path.create(value))
        .map((traceId, path) -> KeyValue.pair(path.path(), path))
        .through(pathTopicName, Produced.with(Serdes.String(), pathSerde))
        .groupByKey()
        .windowedBy(TimeWindows.of(Duration.ofMinutes(1)).grace(Duration.ZERO))
        .aggregate(PathAggregate::new,
            (key, path, aggregate) -> aggregate.add(path),
            Materialized.with(Serdes.String(), pathAggregateSerde))
        .suppress(untilWindowCloses(unbounded()))
        .toStream()
        .foreach((key, value) ->
            System.out.println(("from: " + key.window().startTime())
                + (" to: " + key.window().endTime())
                + (" path: " + value.path)
                + (" counter: " + value.callCount)
                + (" error: " + value.errorCount)));
    return builder.build();
  }
}
