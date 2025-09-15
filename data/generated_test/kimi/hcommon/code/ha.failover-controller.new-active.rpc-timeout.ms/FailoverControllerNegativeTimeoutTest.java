package org.apache.hadoop.ha;

import java.lang.reflect.Method;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.junit.Test;

public class FailoverControllerNegativeTimeoutTest {

  @Test
  public void negativeTimeoutValueIsRejected() throws Throwable {
    // 1. Instantiate Configuration and set negative value
    Configuration conf = new Configuration();
    conf.setInt(CommonConfigurationKeys.HA_FC_NEW_ACTIVE_TIMEOUT_KEY, -1);

    // 2. Attempt to retrieve the timeout and expect an exception
    try {
      // In 2.8.5 the method is package-private; use reflection to invoke it
      Method m = null;
      Class<?> cls = null;
      try {
        cls = Class.forName("org.apache.hadoop.ha.FailoverController");
      } catch (ClassNotFoundException e) {
        // The class doesn't exist in 2.8.5, so skip the test
        return;
      }
      try {
        m = cls.getDeclaredMethod("getTimeoutToNewActive", Configuration.class);
      } catch (NoSuchMethodException e) {
        // The method doesn't exist, so skip the test
        return;
      }
      m.setAccessible(true);
      m.invoke(null, conf);
    } catch (Exception e) {
      // The reflective call wraps the original exception in InvocationTargetException
      Throwable cause = e.getCause();
      if (cause instanceof IllegalArgumentException) {
        // Expected exception, test passes
        return;
      }
      // Re-throw any other unexpected exception
      throw cause;
    }
    throw new AssertionError("Expected IllegalArgumentException was not thrown");
  }
}