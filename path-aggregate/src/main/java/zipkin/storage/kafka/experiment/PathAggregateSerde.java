package zipkin.storage.kafka.experiment;

import com.google.gson.Gson;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

public class PathAggregateSerde implements Serde<PathAggregate> {
  Gson gson = new Gson();

  @Override public Serializer<PathAggregate> serializer() {
    return (topic, data) -> gson.toJson(data).getBytes();
  }

  @Override public Deserializer<PathAggregate> deserializer() {
    return (topic, data) -> gson.fromJson(new String(data), PathAggregate.class);
  }
}
