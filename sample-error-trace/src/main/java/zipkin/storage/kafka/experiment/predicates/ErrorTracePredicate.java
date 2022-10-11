package zipkin.storage.kafka.experiment.predicates;

import java.util.Collection;
import java.util.function.Predicate;
import zipkin2.Span;

public class ErrorTracePredicate implements Predicate<Collection<Span>> {
  @Override
  public boolean test(Collection<Span> spans) {
    return spans.stream().anyMatch(s -> s.tags().containsKey("error"));
  }
}
