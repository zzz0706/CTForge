package org.apache.hadoop.http;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.http.lib.StaticUserWebFilter;
import org.junit.Test;

import static org.junit.Assert.*;

public class TestHttpFilterInitializersConfig {

  @Test
  public void testValidFilterInitializerClass() {
    Configuration conf = new Configuration(false);
    // rely on the default value injected by the runtime
    conf.set("hadoop.http.filter.initializers",
             "org.apache.hadoop.http.lib.StaticUserWebFilter");
    Class<?>[] classes = conf.getClasses("hadoop.http.filter.initializers");
    assertNotNull("Default value must resolve to at least one class", classes);
    assertTrue("Default value must contain StaticUserWebFilter",
               classes.length >= 1);
    boolean found = false;
    for (Class<?> c : classes) {
      if (StaticUserWebFilter.class.equals(c)) {
        found = true;
        break;
      }
    }
    assertTrue("Default value must contain StaticUserWebFilter", found);
  }

  @Test
  public void testInvalidClassName() {
    Configuration conf = new Configuration(false);
    conf.set("hadoop.http.filter.initializers", "com.example.NonExistentFilter");
    try {
      conf.getClasses("hadoop.http.filter.initializers");
      fail("Expected RuntimeException for non-existent class");
    } catch (RuntimeException e) {
      // expected
    }
  }

  @Test
  public void testClassNotAssignableToFilterInitializer() {
    Configuration conf = new Configuration(false);
    conf.set("hadoop.http.filter.initializers", "java.lang.String");
    Class<?>[] classes = conf.getClasses("hadoop.http.filter.initializers");
    assertNotNull("Non-FilterInitializer class still returns Class[]", classes);
    assertEquals("Should return one class", 1, classes.length);
    assertEquals("Class should be java.lang.String", String.class, classes[0]);
  }

  @Test
  public void testEmptyValue() {
    Configuration conf = new Configuration(false);
    conf.set("hadoop.http.filter.initializers", "");
    Class<?>[] classes = conf.getClasses("hadoop.http.filter.initializers");
    assertNotNull("Empty value returns empty array, not null", classes);
    assertEquals("Empty value results in zero-length array", 0, classes.length);
  }

  @Test
  public void testMultipleValidClasses() {
    Configuration conf = new Configuration(false);
    conf.set("hadoop.http.filter.initializers",
             "org.apache.hadoop.http.lib.StaticUserWebFilter,"
           + "org.apache.hadoop.http.lib.StaticUserWebFilter");
    Class<?>[] classes = conf.getClasses("hadoop.http.filter.initializers");
    assertNotNull("Valid comma-separated list must resolve", classes);
    assertEquals("Must resolve two classes", 2, classes.length);
    for (Class<?> c : classes) {
      assertTrue("Each class must extend FilterInitializer",
                 FilterInitializer.class.isAssignableFrom(c));
    }
  }

  @Test
  public void testWhitespaceAroundComma() {
    Configuration conf = new Configuration(false);
    conf.set("hadoop.http.filter.initializers",
             " org.apache.hadoop.http.lib.StaticUserWebFilter , "
           + "org.apache.hadoop.http.lib.StaticUserWebFilter ");
    Class<?>[] classes = conf.getClasses("hadoop.http.filter.initializers");
    assertNotNull("Whitespace around comma must be trimmed", classes);
    assertEquals("Must resolve two classes", 2, classes.length);
  }
}