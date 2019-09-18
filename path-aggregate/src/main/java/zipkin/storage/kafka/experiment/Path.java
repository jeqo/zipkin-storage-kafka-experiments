package zipkin.storage.kafka.experiment;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import zipkin2.Span;
import zipkin2.internal.SpanNode;

public class Path {
  final static Logger logger = Logger.getLogger(Path.class.getName());
  final static SpanNode.Builder builder = SpanNode.newBuilder(logger);

  final List<Step> steps;
  final boolean error;
  final long duration;

  static Path create(List<Span> spans) {
    return new Path(steps(spans), error(spans), duration(spans));
  }

  private static boolean error(List<Span> spans) {
    return spans.stream().anyMatch(span -> span.tags().containsKey("error"));
  }

  static List<Step> steps(List<Span> spans) {

    SpanNode traceTree = builder.build(spans);

    List<Step> steps = new ArrayList<>();
    for (Iterator<SpanNode> i = traceTree.traverse(); i.hasNext(); ) {
      SpanNode current = i.next();
      Span currentSpan = current.span();
      Span.Kind kind = currentSpan.kind();
      String serviceName = currentSpan.localServiceName();
      String spanName = currentSpan.name();
      if (current == traceTree) {
        steps.add(new Step(serviceName, spanName));
        continue;
      }
      if (kind == null) kind = Span.Kind.CLIENT;
      switch (kind) {
        case SERVER:
          steps.add(new Step(serviceName, spanName));
          break;
        case CONSUMER:
          steps.add(new Step(serviceName, spanName));
          break;
        case CLIENT:
        case PRODUCER:
          String remoteServiceName = currentSpan.remoteServiceName();
          //FIXME when messaging abstraction standarize messaging.channel_name
          String remoteChannelName = currentSpan.tags().get("kafka.topic");
          if (remoteChannelName != null) {
            steps.add(new Step(remoteServiceName, remoteChannelName));
          }
          break;
        default:
          logger.fine("unknown kind; skipping");
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

    for (int i = 1; i < filtered.size(); i++) {
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
