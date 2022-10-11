package zipkin.storage.kafka.experiment.predicates;

import org.junit.Test;

import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;

public class TraceSamplingPredicateTest extends TracePredicateTestBase {

  @Test
  public void acceptTraceId() {
    final TraceSamplingPredicate predicate = new TraceSamplingPredicate(1.f);

    final boolean sampled = predicate.test(asList(errorSpanOfTrace1, normalSpanOfTrace1));

    assertThat(sampled).isTrue();
  }

  @Test
  public void acceptDebugTrace() {
    final TraceSamplingPredicate predicate = new TraceSamplingPredicate(0.f);

    final boolean sampled = predicate.test(asList(debugSpan("1"), normalSpanOfTrace1));

    assertThat(sampled).isTrue();
  }

  @Test
  public void discardTraceId() {
    final TraceSamplingPredicate predicate = new TraceSamplingPredicate(0.f);

    final boolean sampled = predicate.test(asList(errorSpanOfTrace1, normalSpanOfTrace1));

    assertThat(sampled).isFalse();
  }

  @Test(expected = IllegalArgumentException.class)
  public void throwsOnInvalidRate() {
    new TraceSamplingPredicate(10.f);
  }

  @Test(expected = IllegalArgumentException.class)
  public void throwsOnNegativeRate() {
    new TraceSamplingPredicate(-10.f);
  }
}
