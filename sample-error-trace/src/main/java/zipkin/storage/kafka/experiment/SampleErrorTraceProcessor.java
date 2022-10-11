package zipkin.storage.kafka.experiment;

import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.common.HttpStatus;
import com.linecorp.armeria.common.MediaType;
import com.linecorp.armeria.server.Server;
import com.linecorp.armeria.server.ServerBuilder;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.binder.jvm.ClassLoaderMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmGcMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmMemoryMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmThreadMetrics;
import io.micrometer.core.instrument.binder.kafka.KafkaMetrics;
import io.micrometer.core.instrument.binder.system.ProcessorMetrics;
import io.micrometer.prometheus.PrometheusConfig;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import java.util.Collection;
import java.util.Properties;
import java.util.function.Predicate;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import zipkin.storage.kafka.experiment.predicates.CompositeTracePredicate;
import zipkin.storage.kafka.experiment.predicates.ErrorTracePredicate;
import zipkin.storage.kafka.experiment.predicates.SlowTracePredicate;
import zipkin.storage.kafka.experiment.predicates.TraceSamplingPredicate;
import zipkin2.Span;

import static java.util.Arrays.asList;

public class SampleErrorTraceProcessor {

  final String traceTopicName;
  final String errorTraceTopicName;

  final Properties streamsConfig;
  final Predicate<Collection<Span>> traceSamplingPredicate;

  static Builder newBuilder() {
    return new Builder();
  }

  SampleErrorTraceProcessor(Builder builder) {
    this.traceTopicName = builder.traceTopicName;
    this.errorTraceTopicName = builder.errorTraceTopicName;
    this.streamsConfig = builder.streamsConfig;
    this.traceSamplingPredicate = builder.traceSamplingPredicate;
  }

  public static void main(String[] args) {
    Config config = ConfigFactory.load();
    SampleErrorTraceProcessor processor = SampleErrorTraceProcessor.newBuilder()
        .bootstrapServers(config.getString("kafka-streams.bootstrap-servers"))
        .applicationId(config.getString("kafka-streams.application-id"))
        .traceTopicName(config.getString("kafka-topics.trace-topic"))
        .errorTraceTopicName(config.getString("kafka-topics.error-trace-topic"))
        .traceSamplingPredicate(new CompositeTracePredicate(asList(
            new ErrorTracePredicate(),
            new SlowTracePredicate(1_000_000L),
            new TraceSamplingPredicate(0.1f))
        ))
        .build();
    processor.run();
  }

  void run() {
    PrometheusMeterRegistry meterRegistry =
        new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
    new JvmThreadMetrics().bindTo(meterRegistry);
    new JvmGcMetrics().bindTo(meterRegistry);
    new JvmMemoryMetrics().bindTo(meterRegistry);
    new ClassLoaderMetrics().bindTo(meterRegistry);
    new ProcessorMetrics().bindTo(meterRegistry);
    Metrics.addRegistry(meterRegistry);
    Topology topology = new SampleErrorTraceTopology(this).get();
    KafkaStreams kafkaStreams = new KafkaStreams(topology, streamsConfig);
    kafkaStreams.start();
    new KafkaMetrics(kafkaStreams).bindTo(meterRegistry);
    Server server = new ServerBuilder()
        .http(8080)
        .service("/", (ctx, req) -> HttpResponse.of("OK"))
        .service("/health", (ctx, req) -> {
          String content = kafkaStreams.state().name() + "\n";
          if (kafkaStreams.state().isRunning()) {
            return HttpResponse.of(MediaType.PLAIN_TEXT_UTF_8, content);
          } else {
            return HttpResponse.of(HttpStatus.INTERNAL_SERVER_ERROR, MediaType.PLAIN_TEXT_UTF_8,
                content);
          }
        })
        .service("/metrics", (ctx, req) -> HttpResponse.of(meterRegistry.scrape()))
        .build();
    server.start();
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      server.close();
      kafkaStreams.close();
      meterRegistry.close();
    }));
  }

  static class Builder {

    String traceTopicName = "zipkin-trace";
    String errorTraceTopicName = "zipkin-error-trace";

    String bootstrapServers = "localhost:19092";
    String applicationId = "zipkin-sample-error-trace-processor";

    Properties streamsConfig = new Properties();
    Predicate<Collection<Span>> traceSamplingPredicate;

    Builder() {
      streamsConfig.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
      streamsConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
      streamsConfig.put(StreamsConfig.consumerPrefix(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG), "earliest");
    }

    Builder bootstrapServers(String bootstrapServers) {
      if (bootstrapServers == null) throw new NullPointerException("bootstrapServers == null");
      streamsConfig.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
      return this;
    }

    Builder applicationId(String applicationId) {
      if (applicationId == null) throw new NullPointerException("applicationId == null");
      streamsConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
      return this;
    }

    SampleErrorTraceProcessor build() {
      return new SampleErrorTraceProcessor(this);
    }

    Builder traceTopicName(String traceTopicName) {
      if (traceTopicName == null) throw new NullPointerException("traceTopicName == null");
      this.traceTopicName = traceTopicName;
      return this;
    }

    Builder errorTraceTopicName(String errorTraceTopicName) {
      if (errorTraceTopicName == null) {
        throw new NullPointerException("errorTraceTopicName == null");
      }
      this.errorTraceTopicName = errorTraceTopicName;
      return this;
    }

    Builder traceSamplingPredicate(Predicate<Collection<Span>> traceSamplingPredicate) {
      this.traceSamplingPredicate = traceSamplingPredicate;
      return this;
    }
  }
}
