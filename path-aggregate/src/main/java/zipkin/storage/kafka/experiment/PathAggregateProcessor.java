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
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;

public class PathAggregateProcessor {

  final String traceTopicName;
  final String pathTopicName;

  final Properties streamsConfig;

  static Builder newBuilder() {
    return new Builder();
  }

  PathAggregateProcessor(Builder builder) {
    this.traceTopicName = builder.traceTopicName;
    this.pathTopicName = builder.pathTopicName;
    this.streamsConfig = builder.streamsConfig;
  }

  public static void main(String[] args) {
    Config config = ConfigFactory.load();
    PathAggregateProcessor processor = PathAggregateProcessor.newBuilder()
        .bootstrapServers(config.getString("kafka-streams.bootstrap-servers"))
        .applicationId(config.getString("kafka-streams.application-id"))
        .traceTopicName(config.getString("kafka-topics.trace-topic"))
        .pathTopicName(config.getString("kafka-topics.path-topic"))
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
    Topology topology = new PathAggregateTopology(this).get();
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
    String pathTopicName = "zipkin-path";

    String bootstrapServers = "localhost:19092";
    String applicationId = "zipkin-path-aggregate-processor";

    Properties streamsConfig = new Properties();

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

    PathAggregateProcessor build() {
      return new PathAggregateProcessor(this);
    }

    Builder traceTopicName(String traceTopicName) {
      if (traceTopicName == null) throw new NullPointerException("traceTopicName == null");
      this.traceTopicName = traceTopicName;
      return this;
    }

    Builder pathTopicName(String errorTraceTopicName) {
      if (errorTraceTopicName == null) {
        throw new NullPointerException("pathTopicName == null");
      }
      this.pathTopicName = errorTraceTopicName;
      return this;
    }
  }
}
