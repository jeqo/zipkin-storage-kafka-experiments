package zipkin.storage.kafka.experiment.predicates;

import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;

public class CompositeTracePredicateTest extends TracePredicateTestBase {
  @Test
  public void acceptIfAnyTestPassed() {
    final CompositeTracePredicate predicate = new CompositeTracePredicate(
      asList(spans -> false, spans -> true)
    );

    final boolean sampled = predicate.test(asList(errorSpanOfTrace1, normalSpanOfTrace1));

    assertThat(sampled).isTrue();
  }

  @Test
  public void discardIfAllTestFailed() {
    final CompositeTracePredicate predicate = new CompositeTracePredicate(
      asList(spans -> false, spans -> false)
    );

    final boolean sampled = predicate.test(asList(errorSpanOfTrace1, normalSpanOfTrace1));

    assertThat(sampled).isFalse();
  }
}
