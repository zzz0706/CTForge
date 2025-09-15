package alluxio.client.file.options;

import alluxio.client.file.FileSystemContext;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.grpc.CreateFilePOptions;
import alluxio.util.FileSystemOptions;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class DefaultValuePropagationToCreateFilePOptionsTest {

  @Test
  public void defaultValuePropagationToCreateFilePOptions() {
    // 1. Obtain a fresh AlluxioConfiguration without any explicit overrides
    AlluxioConfiguration conf = InstancedConfiguration.defaults();

    // 2. Compute the expected value dynamically from the configuration
    int expectedReplicationMin = conf.getInt(PropertyKey.USER_FILE_REPLICATION_MIN);

    // 3. Build the protobuf options via the utility method under test
    CreateFilePOptions options = FileSystemOptions.createFileDefaults(conf);

    // 4. Assert that the replication min field matches the expected value
    assertEquals(expectedReplicationMin, options.getReplicationMin());
  }
}