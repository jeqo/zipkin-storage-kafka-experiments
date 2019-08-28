package zipkin.storage.kafka.experiment;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import zipkin2.Span;

public class Path {
  final List<Step> steps;
  final boolean error;
  final long duration;

  static Path create(List<Span> spans) {
    return new Path(steps(spans), error(spans), duration(spans));
  }

  private static boolean error(List<Span> spans) {
    return spans.stream().anyMatch(span -> !span.tags().getOrDefault("error", "").isEmpty());
  }

  static List<Step> steps(List<Span> spans) {
    List<Step> steps = new ArrayList<>();
    String serviceName = null;
    for (Span span : spans) {
      if (!span.localServiceName().equals(serviceName)) {
        Step step = Step.from(span);
        steps.add(step);
        serviceName = span.localServiceName();
      }
    }
    return steps;
  }

  static long duration(List<Span> spans) {
    List<Span> filtered =
        spans.stream().filter(s -> Objects.nonNull(s.duration()))
            .sorted(Comparator.comparing(Span::timestampAsLong))
            .collect(Collectors.toList());

    if (filtered.isEmpty()) return 0;
    if (filtered.size() == 1) return filtered.get(0).durationAsLong();

    long result = filtered.get(0).durationAsLong();
    long currentIntervalEnd = filtered.get(0).timestampAsLong() + filtered.get(0).durationAsLong();

    for (int i = 1; i < filtered.size(); i ++) {
      Span next = filtered.get(i);
      long nextIntervalEnd = next.timestampAsLong() + next.durationAsLong();

      if (nextIntervalEnd <= currentIntervalEnd) { // we are still in the interval
        continue;
      } else if (next.timestampAsLong() <= currentIntervalEnd) { // we extending the interval
        result += nextIntervalEnd - currentIntervalEnd;
        currentIntervalEnd = nextIntervalEnd;
      } else { // this is a new interval
        result += next.durationAsLong();
        currentIntervalEnd = nextIntervalEnd;
      }
    }

    return result;
  }

  Path(List<Step> steps, boolean error, long duration) {
    this.steps = steps;
    this.error = error;
    this.duration = duration;
  }

  String path() {
    return steps.stream().map(Step::step).collect(Collectors.joining("|"));
  }

  static class Step {
    final String serviceName;
    final String operation;

    Step(String serviceName, String operation) {
      this.serviceName = serviceName;
      this.operation = operation;
    }

    public static Step from(Span span) {
      return new Step(span.localServiceName(), span.name());
    }

    public String step() {
      return String.format("%s:%s", serviceName, operation);
    }
  }
}
