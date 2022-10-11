package zipkin.storage.kafka.experiment.predicates;

import org.junit.Ignore;
import zipkin2.Span;

@Ignore
class TracePredicateTestBase {
  final Span errorSpanOfTrace1 = errorSpan("1");
  final Span normalSpanOfTrace1 = normalSpan("1");

  private Span errorSpan(String traceId) {
    return Span.newBuilder()
      .traceId(traceId).id("1").name("get").duration(0L)
      .putTag("error", "500")
      .build();
  }

  private Span normalSpan(String traceId) {
    return Span.newBuilder()
      .traceId(traceId).id("2").name("get").duration(0L)
      .build();
  }

  Span debugSpan(String traceId) {
    return Span.newBuilder()
      .traceId(traceId).id("3").name("get").duration(0L).debug(true)
      .build();
  }

  Span slowSpan(String traceId) {
    return Span.newBuilder()
      .traceId(traceId).id("4").name("get").duration(10_000_000L)
      .build();
  }
}
