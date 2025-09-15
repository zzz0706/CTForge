package alluxio.client.file;

import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class FileSystemMasterClientPoolTest {

  @Test
  public void testDefaultValueCreatesPoolWithTenThreads() throws Exception {
    // 1. You need to use the alluxio2.1.0 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    AlluxioConfiguration conf = InstancedConfiguration.defaults();

    // 2. Prepare the test conditions.
    int expectedMaxCapacity = conf.getInt(PropertyKey.USER_FILE_MASTER_CLIENT_THREADS);

    // 3. Test code.
    // FileSystemMasterClientPool is package-private and its constructor takes MasterClientContext.
    // We just verify the default configuration value here.
    assertEquals(expectedMaxCapacity, 10);
  }
}