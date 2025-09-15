package org.apache.hadoop.security;

import static org.junit.Assert.assertTrue;

import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

public class TestKerberosMinSecondsBeforeReloginConfig {

  @Test
  public void testValidKerberosMinSecondsBeforeRelogin() {
    // 1. Obtain configuration value from the file, no hard-coding
    Configuration conf = new Configuration();
    String key = "hadoop.kerberos.min.seconds.before.relogin";
    String value = conf.get(key);

    // 2. Prepare test conditions – treat missing as “60” (the default)
    long parsed;
    if (value == null) {
      parsed = 60L; // documented default
    } else {
      try {
        parsed = Long.parseLong(value.trim());
      } catch (NumberFormatException nfe) {
        throw new IllegalArgumentException(
            "Invalid value for " + key + ": " + value);
      }
    }

    // 3. Test code – verify constraints
    // Must be a positive long (milliseconds used in code are positive)
    assertTrue("hadoop.kerberos.min.seconds.before.relogin must be > 0",
        parsed > 0);

    // 4. Code after testing – nothing to tear down
  }
}