package zipkin.storage.kafka.experiment.predicates;

import java.util.Collection;
import java.util.function.Predicate;
import zipkin2.Span;

public class CompositeTracePredicate implements Predicate<Collection<Span>> {
  private final Collection<Predicate<Collection<Span>>> predicates;

  public CompositeTracePredicate(Collection<Predicate<Collection<Span>>> predicates) {
    this.predicates = predicates;
  }

  @Override
  public boolean test(Collection<Span> spans) {
    for (Predicate<Collection<Span>> predicate : predicates) {
      if (predicate.test(spans)) {
        return true;
      }
    }
    return false;
  }
}
