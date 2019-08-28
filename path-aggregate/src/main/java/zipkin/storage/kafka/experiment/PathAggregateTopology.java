package zipkin.storage.kafka.experiment;

import java.time.Duration;
import java.util.List;
import java.util.function.Supplier;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.TimeWindows;
import zipkin2.Span;
import zipkin2.storage.kafka.streams.serdes.SpansSerde;

public class PathAggregateTopology implements Supplier<Topology> {

  final String traceTopicName;
  final String errorTraceTopicName;

  final SpansSerde spansSerde;
  final PathSerde pathSerde;
  final PathAggregateSerde pathAggregateSerde;

  PathAggregateTopology(PathAggregateProcessor processor) {
    this.traceTopicName = processor.traceTopicName;
    this.errorTraceTopicName = processor.errorTraceTopicName;
    spansSerde = new SpansSerde();
    pathSerde = new PathSerde();
    pathAggregateSerde = new PathAggregateSerde();
  }

  public Topology get() {
    StreamsBuilder builder = new StreamsBuilder();
    builder.stream(traceTopicName, Consumed.with(Serdes.String(), spansSerde))
        .mapValues((readOnlyKey, value) -> Path.create(value))
        .map((traceId, path) -> KeyValue.pair(path.path(), path))
        .through(errorTraceTopicName, Produced.with(Serdes.String(), pathSerde))
        .groupByKey()
        .windowedBy(TimeWindows.of(Duration.ofMinutes(1)).grace(Duration.ZERO))
        .aggregate(PathAggregate::new,
            (key, path, aggregate) -> aggregate.add(path));
    return builder.build();
  }

  private boolean isError(List<Span> spans) {
    return spans.stream().anyMatch(span ->
        !span.tags().getOrDefault("error", "").isEmpty());
  }
}
