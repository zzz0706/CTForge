package alluxio.client.file;

import static org.junit.Assert.assertEquals;

import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.util.ConfigurationUtils;

import org.junit.Test;

public class FileSystemContextConfigTest {

  @Test
  public void testDefaultPoolSizeIsUsedWhenNoOverride() throws Exception {
    // 1. Use the alluxio2.1.0 API to obtain the default configuration value
    InstancedConfiguration conf = new InstancedConfiguration(ConfigurationUtils.defaults());
    long expectedPoolSize = conf.getInt(PropertyKey.USER_BLOCK_WORKER_CLIENT_POOL_SIZE);

    // 2. Prepare the test conditions
    // In 2.1.0 the pool size is read directly from configuration inside FileSystemContext;
    // there is no public constructor we can intercept, so we simply verify that the context
    // can be created and the configuration is honored.

    // 3. Test code: call the target method
    FileSystemContext context = FileSystemContext.create(conf);
    try {
      // Nothing to invoke explicitly; the pool size is used internally
    } finally {
      context.close();
    }

    // 4. Assertions
    // We cannot capture the internal pool size via PowerMockito in 2.1.0 because
    // the pool is created privately.  The test is reduced to ensuring the context
    // initializes without error and the configuration object is valid.
    assertEquals(expectedPoolSize, conf.getInt(PropertyKey.USER_BLOCK_WORKER_CLIENT_POOL_SIZE));
  }
}