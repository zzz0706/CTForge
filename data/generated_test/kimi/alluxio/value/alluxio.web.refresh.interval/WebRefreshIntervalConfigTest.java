package alluxio.conf;

import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;

import org.junit.Assert;
import org.junit.Test;

public class WebRefreshIntervalConfigTest {

  @Test
  public void testWebRefreshIntervalValid() {
    // 1. You need to use the alluxio2.1.0 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    AlluxioConfiguration conf = new InstancedConfiguration(new AlluxioProperties());

    // 2. Prepare the test conditions.
    //    No additional setup needed; InstancedConfiguration is initialized with defaults.

    // 3. Test code.
    //    Constraint: the value must be a positive integer (milliseconds).
    long intervalMs = conf.getMs(PropertyKey.WEB_REFRESH_INTERVAL);
    Assert.assertTrue("alluxio.web.refresh.interval must be a positive duration",
                      intervalMs > 0);

    // 4. Code after testing.
    //    No teardown required; InstancedConfiguration is local.
  }
}