package org.apache.hadoop.security;

import static org.junit.Assert.assertEquals;

import org.apache.hadoop.conf.Configuration;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class UserGroupInformationConfigTest {

  @Before
  public void setUp() {
    // Reset UGI so that the next call to ensureInitialized() will re-parse the config
    UserGroupInformation.reset();
  }

  @After
  public void tearDown() {
    // Clean up after test
    UserGroupInformation.reset();
  }

  @Test
  public void verifyDefaultMinSecondsBeforeReloginIsUsedWhenNoConfigOverride() throws Exception {
    // 1. Create fresh Configuration without overrides
    Configuration conf = new Configuration();

    // 2. Compute expected value dynamically from the same Configuration instance
    long expectedMs = 1000L * conf.getLong(
            "hadoop.kerberos.min.seconds.before.relogin",
            60L);

    // 3. Trigger initialization via setConfiguration
    UserGroupInformation.setConfiguration(conf);

    // 4. Assert the internal static field is correctly set
    //    Use reflection to read the private field
    long actualMs = 0L;
    try {
      java.lang.reflect.Field field =
              UserGroupInformation.class.getDeclaredField("kerberosMinSecondsBeforeRelogin");
      field.setAccessible(true);
      actualMs = field.getLong(null);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    assertEquals("kerberosMinSecondsBeforeRelogin should equal default 60000L",
            expectedMs, actualMs);
  }
}