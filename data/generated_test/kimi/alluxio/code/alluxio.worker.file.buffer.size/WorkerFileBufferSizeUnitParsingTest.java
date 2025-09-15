package alluxio.conf;

import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.util.ConfigurationUtils;
import org.junit.After;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class WorkerFileBufferSizeUnitParsingTest {

  @Test
  public void unitParsingWorksForGB() throws Exception {
    // 1. Use the Alluxio 2.1.0 API to read the configuration value.
    InstancedConfiguration conf = new InstancedConfiguration(ConfigurationUtils.defaults());
    conf.set(PropertyKey.WORKER_FILE_BUFFER_SIZE, "2GB");

    // 2. Compute the expected value dynamically.
    long expectedBytes = 2L * 1024 * 1024 * 1024;

    // 3. Call the target method (ServerConfiguration.getBytes) under test.
    long actualBytes = conf.getBytes(PropertyKey.WORKER_FILE_BUFFER_SIZE);

    // 4. Assert the parsed value matches the expectation.
    assertEquals(expectedBytes, actualBytes);
  }

  @After
  public void after() {
    // Reset configuration to avoid side-effects on other tests
  }
}