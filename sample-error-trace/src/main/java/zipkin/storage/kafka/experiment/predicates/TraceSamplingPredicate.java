package zipkin.storage.kafka.experiment.predicates;

import java.util.Collection;
import java.util.function.Predicate;
import zipkin2.Span;
import zipkin2.internal.HexCodec;

public class TraceSamplingPredicate implements Predicate<Collection<Span>> {
  private final float rate;

  public TraceSamplingPredicate(float rate) {
    if (rate < 0.f || rate > 1.f)
      throw new IllegalArgumentException("rate should be between 0 and 1: was " + rate);

    this.rate = rate;
  }

  @Override
  public boolean test(Collection<Span> spans) {
    return spans.stream()
      .anyMatch(span -> {
        if (Boolean.TRUE.equals(span.debug())) return true;
        long traceId = HexCodec.lowerHexToUnsignedLong(span.traceId());
        // The absolute value of Long.MIN_VALUE is larger than a long, so Math.abs returns identity.
        // This converts to MAX_VALUE to avoid always dropping when traceId == Long.MIN_VALUE
        long t = traceId == Long.MIN_VALUE ? Long.MAX_VALUE : Math.abs(traceId);
        return t <= (long) (Long.MAX_VALUE * rate);
      });
  }
}
