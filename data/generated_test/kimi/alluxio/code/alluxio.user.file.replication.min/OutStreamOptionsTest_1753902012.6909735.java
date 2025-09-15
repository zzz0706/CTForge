package alluxio.client.file.options;

import alluxio.ClientContext;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.AlluxioProperties;
import alluxio.conf.PropertyKey;
import alluxio.util.FileSystemOptions;
import alluxio.grpc.CreateFilePOptions;

import org.junit.Test;
import static org.junit.Assert.assertEquals;

public class OutStreamOptionsTest {

  @Test
  public void testDefaultValuePropagationToOutStreamOptions() throws Exception {
    // 1. Configuration as Input – use a fresh, un-modified AlluxioConfiguration
    AlluxioConfiguration conf = new InstancedConfiguration(new AlluxioProperties());

    // 2. Dynamic Expected Value Calculation – read the default directly from conf
    int expectedReplicationMin = conf.getInt(PropertyKey.USER_FILE_REPLICATION_MIN);

    // 3. Prepare test conditions – use ClientContext.create(conf) to avoid external deps
    ClientContext context = ClientContext.create(conf);

    // 4. Invoke the method under test
    OutStreamOptions options = OutStreamOptions.defaults(context);

    // 5. Assertions and Verification
    assertEquals(expectedReplicationMin, options.getReplicationMin());
  }

  @Test
  public void testCreateFileDefaultsUsesConfiguration() {
    // 1. Configuration as Input
    AlluxioConfiguration conf = new InstancedConfiguration(new AlluxioProperties());

    // 2. Dynamic Expected Value Calculation
    int expectedReplicationMin = conf.getInt(PropertyKey.USER_FILE_REPLICATION_MIN);

    // 3. Invoke the method under test
    CreateFilePOptions options = FileSystemOptions.createFileDefaults(conf);

    // 4. Assertions and Verification
    assertEquals(expectedReplicationMin, options.getReplicationMin());
  }
}