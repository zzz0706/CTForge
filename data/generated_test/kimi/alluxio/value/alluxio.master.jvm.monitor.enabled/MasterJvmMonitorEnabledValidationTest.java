package alluxio.master;

import static org.junit.Assert.assertTrue;

import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.util.ConfigurationUtils;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class MasterJvmMonitorEnabledValidationTest {

  private InstancedConfiguration mConf;

  @Before
  public void before() {
    mConf = new InstancedConfiguration(ConfigurationUtils.defaults());
  }

  @After
  public void after() {
    // reset to default
    mConf.unset(PropertyKey.MASTER_JVM_MONITOR_ENABLED);
  }

  /**
   * Validates that the value of {@link PropertyKey#MASTER_JVM_MONITOR_ENABLED} is a valid
   * boolean string ("true" or "false").
   */
  @Test
  public void validateMasterJvmMonitorEnabled() {
    boolean value = mConf.getBoolean(PropertyKey.MASTER_JVM_MONITOR_ENABLED);
    assertTrue("alluxio.master.jvm.monitor.enabled must be a valid boolean (true or false)",
        Boolean.TRUE.equals(value) || Boolean.FALSE.equals(value));
  }
}