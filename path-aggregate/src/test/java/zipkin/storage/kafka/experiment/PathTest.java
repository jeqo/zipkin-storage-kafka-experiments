package zipkin.storage.kafka.experiment;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.Test;
import zipkin2.Endpoint;
import zipkin2.Span;

import static org.junit.jupiter.api.Assertions.*;

class PathTest {

  @Test void emptyPath_emptySpans() {
    Path path = Path.create(Collections.emptyList());

    assertEquals("", path.path());
  }

  @Test void createPath_oneSpanWithDurationAndTimestamp() {
    List<Span> spans = Collections.singletonList(Span.newBuilder()
        .traceId("a")
        .id("a")
        .name("op-a")
        .localEndpoint(Endpoint.newBuilder().serviceName("svc-a").build())
        .duration(Duration.ofSeconds(2).toMillis() * 1000)
        .timestamp(System.currentTimeMillis() * 1000) // to micros
        .build());
    Path path = Path.create(spans);

    assertEquals("svc-a:op-a", path.path());
    assertEquals(2000, path.duration);
  }

  @Test void createPath_oneSpanWithoutDuration() {
    List<Span> spans = Collections.singletonList(Span.newBuilder()
        .traceId("a")
        .id("a")
        .name("op-a")
        .localEndpoint(Endpoint.newBuilder().serviceName("svc-a").build())
        .timestamp(System.currentTimeMillis() * 1000) // to micros
        .build());
    Path path = Path.create(spans);

    assertEquals("svc-a:op-a", path.path());
    assertEquals(0, path.duration);
  }

  @Test void createPath_twoSpans_aggregateDuration() {
    long start = System.currentTimeMillis() * 1000;
    List<Span> spans = Arrays.asList(
        Span.newBuilder()
            .traceId("a")
            .id("a")
            .name("op-a")
            .duration(Duration.ofSeconds(2).toMillis() * 1000)
            .localEndpoint(Endpoint.newBuilder().serviceName("svc-a").build())
            .timestamp(start) // to micros
            .build(),
        Span.newBuilder()
            .traceId("a")
            .id("b")
            .name("op-b")
            .duration(Duration.ofSeconds(2).toMillis() * 1000)
            .localEndpoint(Endpoint.newBuilder().serviceName("svc-b").build())
            .timestamp(start + (Duration.ofSeconds(1).toMillis() * 1000)) // to micros
            .build());
    Path path = Path.create(spans);

    assertEquals("svc-a:op-a|svc-b:op-b", path.path());
    assertEquals(3000 * 1000, path.duration);
  }
}