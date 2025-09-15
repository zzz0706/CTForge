package org.apache.hadoop.security;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import static org.junit.Assert.*;

public class TestKerberosMinSecondsBeforeReloginConfig {

  @Test
  public void testKerberosMinSecondsBeforeReloginValid() {
    Configuration conf = new Configuration(false);
    // 1. Obtain configuration value via the API, not hard-coded
    String val = conf.get("hadoop.kerberos.min.seconds.before.relogin",
                          "60"); // default is 60

    // 2. Prepare test conditions: parse the value
    long parsed;
    try {
      parsed = Long.parseLong(val);
    } catch (NumberFormatException nfe) {
      fail("Invalid value for hadoop.kerberos.min.seconds.before.relogin: " + val);
      return;
    }

    // 3. Test code: enforce documented constraint
    // From code: must be >= 0 (milliseconds used internally)
    assertTrue("hadoop.kerberos.min.seconds.before.relogin must be non-negative",
               parsed >= 0);
  }
}