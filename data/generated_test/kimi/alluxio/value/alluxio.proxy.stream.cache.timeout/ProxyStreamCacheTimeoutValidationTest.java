package alluxio.conf;

import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.util.ConfigurationUtils;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class ProxyStreamCacheTimeoutValidationTest {

  private InstancedConfiguration mConf;

  @Before
  public void before() {
    // Reset the configuration so we read from the actual configuration file
    mConf = new InstancedConfiguration(ConfigurationUtils.defaults());
  }

  @After
  public void after() {
    // Reset the configuration again to avoid side effects on other tests
    mConf = null;
  }

  @Test
  public void proxyStreamCacheTimeoutShouldBePositiveMs() {
    // 1. Obtain the configuration value via the Alluxio 2.1.0 API
    long timeoutMs = mConf.getMs(PropertyKey.PROXY_STREAM_CACHE_TIMEOUT_MS);

    // 2. The timeout must be a positive long value (milliseconds)
    Assert.assertTrue(
        "alluxio.proxy.stream.cache.timeout must be a positive duration in milliseconds",
        timeoutMs > 0);
  }
}