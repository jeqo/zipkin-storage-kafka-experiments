package zipkin.storage.kafka.experiment.predicates;

import java.util.Collection;
import java.util.function.Predicate;
import zipkin2.Span;

public class SlowTracePredicate implements Predicate<Collection<Span>> {
  private final long maxResponseTime;

  public SlowTracePredicate(long maxResponseTime) {
    this.maxResponseTime = maxResponseTime;
  }

  @Override
  public boolean test(Collection<Span> spans) {
    return spans.stream()
      .anyMatch(span -> span.durationAsLong() >= maxResponseTime);
  }
}
