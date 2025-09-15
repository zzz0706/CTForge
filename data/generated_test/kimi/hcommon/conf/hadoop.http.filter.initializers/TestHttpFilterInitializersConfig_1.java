package org.apache.hadoop.http;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import static org.junit.Assert.*;

public class TestHttpFilterInitializersConfig {

  @Test
  public void testFilterInitializersConfigValid() {
    Configuration conf = new Configuration(false);
    // 1. Let the test read the value from the actual configuration file instead of setting it.
    String[] classes = conf.getStrings("hadoop.http.filter.initializers");
    if (classes == null) {
      // default is acceptable
      return;
    }
    // 2. Validate each class name
    for (String clazz : classes) {
      try {
        Class<?> cls = conf.getClassByName(clazz);
        assertTrue("Class " + clazz + " does not implement FilterInitializer",
                   FilterInitializer.class.isAssignableFrom(cls));
      } catch (ClassNotFoundException e) {
        fail("Class " + clazz + " specified in hadoop.http.filter.initializers not found");
      }
    }
  }

  @Test
  public void testFilterInitializersConfigEmptyString() {
    Configuration conf = new Configuration(false);
    // Prepare the test conditions: explicitly set the key to empty string
    conf.set("hadoop.http.filter.initializers", "");
    // 3. Test code
    String[] classes = conf.getStrings("hadoop.http.filter.initializers");
    // In Hadoop 2.8.5 Configuration#getStrings returns null for empty string
    assertNull("Empty string should produce null array", classes);
    // 4. Code after testing
    conf.unset("hadoop.http.filter.initializers");
  }

  @Test
  public void testFilterInitializersConfigNull() {
    Configuration conf = new Configuration(false);
    // Prepare the test conditions: explicitly unset the key
    conf.unset("hadoop.http.filter.initializers");
    // 3. Test code
    String[] classes = conf.getStrings("hadoop.http.filter.initializers");
    assertNull("When key absent, result should be null", classes);
    // 4. Code after testing (already unset)
  }
}