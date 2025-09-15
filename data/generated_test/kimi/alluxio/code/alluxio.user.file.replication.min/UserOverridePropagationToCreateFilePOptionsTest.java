package alluxio.client.file.options;

import static org.junit.Assert.assertEquals;

import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.util.FileSystemOptions;
import alluxio.grpc.CreateFilePOptions;

import org.junit.Test;

public class UserOverridePropagationToCreateFilePOptionsTest {

  @Test
  public void testUserOverridePropagationToCreateFilePOptions() {
    // 1. Instantiate configuration and override the property
    AlluxioConfiguration defaults = InstancedConfiguration.defaults();
    InstancedConfiguration conf = new InstancedConfiguration(defaults.copyProperties());
    conf.set(PropertyKey.USER_FILE_REPLICATION_MIN, 5);

    // 2. Compute expected value dynamically from the configuration
    int expectedReplicationMin = conf.getInt(PropertyKey.USER_FILE_REPLICATION_MIN);

    // 3. Invoke the method under test
    CreateFilePOptions options = FileSystemOptions.createFileDefaults(conf);

    // 4. Assertions
    assertEquals(expectedReplicationMin, options.getReplicationMin());
  }
}