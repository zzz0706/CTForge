package alluxio.client.file.options;

import alluxio.ClientContext;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.util.ConfigurationUtils;
import org.junit.Test;
import org.junit.Assert;

public class OutStreamOptionsTest {

  @Test
  public void userOverridePropagationToOutStreamOptions() {
    // 1. Create a configuration instance and override the key
    InstancedConfiguration conf = new InstancedConfiguration(ConfigurationUtils.defaults());
    conf.set(PropertyKey.USER_FILE_REPLICATION_MIN, 3);

    // 2. Dynamically compute the expected value from the same configuration
    int expectedReplicationMin = conf.getInt(PropertyKey.USER_FILE_REPLICATION_MIN);

    // 3. Prepare ClientContext with the overridden configuration
    ClientContext context = ClientContext.create(conf);

    // 4. Invoke the method under test
    OutStreamOptions options = OutStreamOptions.defaults(context);

    // 5. Assert the propagated value
    Assert.assertEquals(expectedReplicationMin, options.getReplicationMin());
  }
}