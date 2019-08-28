package zipkin.storage.kafka.experiment;

public class PathAggregate {
  String path;
  long errorCount;
  long callCount;

  public PathAggregate add(Path path) {
    if (path.error) this.errorCount ++;
    else this.callCount++;
    this.path = path.path();
    return this;
  }
}
