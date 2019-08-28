package zipkin.storage.kafka.experiment;

import com.google.gson.Gson;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

public class PathSerde implements Serde<Path> {
  Gson gson = new Gson();

  @Override public Serializer<Path> serializer() {
    return (topic, data) -> gson.toJson(data).getBytes();
  }

  @Override public Deserializer<Path> deserializer() {
    return (topic, data) -> gson.fromJson(new String(data), Path.class);
  }
}
