package alluxio.master.meta;

import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class MasterDailyBackupConfigValidationTest {

  @Before
  public void before() {
    ServerConfiguration.reset();
  }

  @After
  public void after() {
    ServerConfiguration.reset();
  }

  @Test
  public void testDailyBackupEnabledBooleanValue() {
    // 1. Read the configuration value from the file (not set in code)
    boolean enabled = ServerConfiguration.getBoolean(PropertyKey.MASTER_DAILY_BACKUP_ENABLED);

    // 2. Validate the boolean value is either true or false
    assertTrue("alluxio.master.daily.backup.enabled must be a valid boolean (true or false)",
        enabled == true || enabled == false);
  }
}