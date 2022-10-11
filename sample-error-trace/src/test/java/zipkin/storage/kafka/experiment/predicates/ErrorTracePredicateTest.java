package zipkin.storage.kafka.experiment.predicates;

import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;

public class ErrorTracePredicateTest extends TracePredicateTestBase {

  private final ErrorTracePredicate predicate = new ErrorTracePredicate();

  @Test
  public void acceptTraceId() {
    final boolean sampled = predicate.test(asList(errorSpanOfTrace1, normalSpanOfTrace1));

    assertThat(sampled).isTrue();
  }

  @Test
  public void discardTraceId() {
    final boolean sampled = predicate.test(asList(slowSpan("1"), normalSpanOfTrace1));

    assertThat(sampled).isFalse();
  }
}
