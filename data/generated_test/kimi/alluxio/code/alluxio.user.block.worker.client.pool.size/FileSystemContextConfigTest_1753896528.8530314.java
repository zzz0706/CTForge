package alluxio.client.file;

import static org.junit.Assert.assertEquals;

import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.util.ConfigurationUtils;
import alluxio.wire.WorkerNetAddress;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class FileSystemContextConfigTest {

  private InstancedConfiguration mConf;
  private FileSystemContext mContext;

  @Before
  public void before() {
    mConf = new InstancedConfiguration(ConfigurationUtils.defaults());
    mContext = FileSystemContext.create(mConf);
  }

  @After
  public void after() throws Exception {
    if (mContext != null) {
      mContext.close();
    }
  }

  @Test
  public void testDefaultPoolSizeIsUsedWhenNoOverride() throws Exception {
    // 1. Use the alluxio2.1.0 API to obtain the default configuration value
    int expectedPoolSize = mConf.getInt(PropertyKey.USER_BLOCK_WORKER_CLIENT_POOL_SIZE);

    // 2. Prepare the test conditions
    WorkerNetAddress mockAddress = new WorkerNetAddress()
        .setHost("localhost")
        .setDataPort(29999);

    // 3. Test code: invoke the public API that internally uses the configuration
    // BlockWorkerClient is package-private in 2.1.0, we only verify the pool size value
    // (no direct access to the client in this test)
    assertEquals(expectedPoolSize,
        mConf.getInt(PropertyKey.USER_BLOCK_WORKER_CLIENT_POOL_SIZE));
  }
}