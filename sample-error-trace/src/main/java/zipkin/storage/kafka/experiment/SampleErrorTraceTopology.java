package zipkin.storage.kafka.experiment;

import java.util.List;
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

  public SampleErrorTraceTopology(String traceTopicName, String errorTraceTopicName) {
    this.traceTopicName = traceTopicName;
    this.errorTraceTopicName = errorTraceTopicName;

    spansSerde = new SpansSerde();
  }

  SampleErrorTraceTopology(SampleErrorTraceProcessor processor) {
    this.traceTopicName = processor.traceTopicName;
    this.errorTraceTopicName = processor.errorTraceTopicName;
    spansSerde = new SpansSerde();
  }

  public Topology get() {
    StreamsBuilder builder = new StreamsBuilder();
    builder.stream(traceTopicName, Consumed.with(Serdes.String(), spansSerde))
        .filter((traceId, spans) -> isError(spans))
        .to(errorTraceTopicName, Produced.with(Serdes.String(), spansSerde));
    return builder.build();
  }

  private boolean isError(List<Span> spans) {
    return spans.stream().anyMatch(span ->
        !span.tags().getOrDefault("error", "").isEmpty());
  }
}
