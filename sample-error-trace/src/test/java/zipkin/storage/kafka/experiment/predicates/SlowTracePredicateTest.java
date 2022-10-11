package zipkin.storage.kafka.experiment.predicates;

import org.junit.Test;

import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;

public class SlowTracePredicateTest extends TracePredicateTestBase {

  private final SlowTracePredicate predicate = new SlowTracePredicate(1_000_000L);

  @Test
  public void acceptTraceId() {
    final boolean sampled = predicate.test(asList(slowSpan("1"), normalSpanOfTrace1));

    assertThat(sampled).isTrue();
  }

  @Test
  public void discardTraceId() {
    final boolean sampled = predicate.test(asList(errorSpanOfTrace1, normalSpanOfTrace1));

    assertThat(sampled).isFalse();
  }
}
