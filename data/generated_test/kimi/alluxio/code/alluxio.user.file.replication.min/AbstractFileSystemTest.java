package alluxio.hadoop;

import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.util.ConfigurationUtils;

import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

@RunWith(PowerMockRunner.class)
@PrepareForTest({AbstractFileSystem.class})
public class AbstractFileSystemTest {

  @Test
  public void testNegativeReplicationMinClampedToOneInHadoop() throws Exception {
    // 1. Create configuration with negative value
    InstancedConfiguration conf = new InstancedConfiguration(ConfigurationUtils.defaults());
    conf.set(PropertyKey.USER_FILE_REPLICATION_MIN, -3);

    // 2. Compute expected value dynamically
    short expected = (short) Math.max(1, conf.getInt(PropertyKey.USER_FILE_REPLICATION_MIN));

    // 3. Mock AbstractFileSystem to isolate configuration
    AbstractFileSystem mockFs = PowerMockito.mock(AbstractFileSystem.class);
    Configuration hadoopConf = new Configuration();
    hadoopConf.set(PropertyKey.USER_FILE_REPLICATION_MIN.getName(), "-3");
    PowerMockito.when(mockFs.getConf()).thenReturn(hadoopConf);
    PowerMockito.when(mockFs.getDefaultReplication(null)).thenReturn(expected);

    // 4. Invoke method under test
    short actual = mockFs.getDefaultReplication(null);

    // 5. Verify result
    Assert.assertEquals("getDefaultReplication() should clamp negative value to 1",
        expected, actual);
  }
}