package zipkin.storage.kafka.experiment;

import java.util.Collection;
import java.util.function.Predicate;
import java.util.function.Supplier;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import zipkin2.Span;
import zipkin2.storage.kafka.streams.serdes.SpansSerde;

public class SampleErrorTraceTopology implements Supplier<Topology> {

  final String traceTopicName;
  final String errorTraceTopicName;

  final SpansSerde spansSerde;
  private final Predicate<Collection<Span>> traceSamplingPredicate;

  public SampleErrorTraceTopology(String traceTopicName,
      String errorTraceTopicName,
      Predicate<Collection<Span>> traceSamplingPredicate) {
    this.traceTopicName = traceTopicName;
    this.errorTraceTopicName = errorTraceTopicName;
    this.traceSamplingPredicate = traceSamplingPredicate;

    spansSerde = new SpansSerde();
  }

  SampleErrorTraceTopology(SampleErrorTraceProcessor processor) {
    this.traceTopicName = processor.traceTopicName;
    this.errorTraceTopicName = processor.errorTraceTopicName;
    this.traceSamplingPredicate = processor.traceSamplingPredicate;
    spansSerde = new SpansSerde();
  }

  public Topology get() {
    StreamsBuilder builder = new StreamsBuilder();
    builder.stream(traceTopicName, Consumed.with(Serdes.String(), spansSerde))
        .filter((traceId, spans) -> traceSamplingPredicate.test(spans))
        .to(errorTraceTopicName, Produced.with(Serdes.String(), spansSerde));
    return builder.build();
  }
}
