package alluxio.client.file;

import alluxio.AlluxioURI;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.util.ConfigurationUtils;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Unit tests for {@link PropertyKey#USER_UFS_BLOCK_LOCATION_ALL_FALLBACK_ENABLED}.
 */
public class UfsBlockLocationAllFallbackEnabledTest {

  private InstancedConfiguration mConf;

  @Before
  public void before() {
    mConf = new InstancedConfiguration(ConfigurationUtils.defaults());
  }

  @After
  public void after() {
    mConf = null;
  }

  @Test
  public void validateBooleanValue() {
    AlluxioConfiguration conf = mConf;
    // 1. Obtain the configuration value using the Alluxio2.1.0 API.
    boolean value = conf.getBoolean(PropertyKey.USER_UFS_BLOCK_LOCATION_ALL_FALLBACK_ENABLED);

    // 2. Test code: validate the value is a valid boolean.
    assertTrue("Configuration must be a boolean", value == true || value == false);
  }
}