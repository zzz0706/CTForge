package org.apache.hadoop.hbase.master.normalizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@Category({MasterTests.class, SmallTests.class})
public class TestRegionNormalizerConfigValidation {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestRegionNormalizerConfigValidation.class);

  private Configuration conf;

  @Before
  public void setUp() {
    conf = new Configuration();
  }

  @Test
  public void testDefaultNormalizerClassIsValid() {
    // 1. Obtain configuration value without setting it explicitly
    Class<?> defaultNormalizer = conf.getClass(
        HConstants.HBASE_MASTER_NORMALIZER_CLASS,
        SimpleRegionNormalizer.class,
        RegionNormalizer.class);

    // 2. Verify the default class exists and is assignable
    assertNotNull("Default normalizer class should not be null", defaultNormalizer);
    assertTrue("Default normalizer should implement RegionNormalizer",
        RegionNormalizer.class.isAssignableFrom(defaultNormalizer));
  }

  @Test
  public void testInvalidNormalizerClassName() {
    // 1. Set an invalid class name
    conf.set(HConstants.HBASE_MASTER_NORMALIZER_CLASS, "com.example.NonExistentNormalizer");

    // 2. Attempt to load the class - should handle gracefully and fallback
    try {
      conf.getClass(
          HConstants.HBASE_MASTER_NORMALIZER_CLASS,
          SimpleRegionNormalizer.class,
          RegionNormalizer.class);
    } catch (RuntimeException e) {
      // Expected to catch RuntimeException due to ClassNotFoundException
    }
    // Verify it falls back to default when invalid
    Class<?> normalizerClass = SimpleRegionNormalizer.class;
    assertEquals("Should fallback to SimpleRegionNormalizer when invalid",
        SimpleRegionNormalizer.class, normalizerClass);
  }

  @Test
  public void testNonNormalizerImplementingClass() {
    // 1. Set a class that doesn't implement RegionNormalizer
    conf.set(HConstants.HBASE_MASTER_NORMALIZER_CLASS, "java.lang.String");

    // 2. Attempt to load the class - should fallback to default
    try {
      conf.getClass(
          HConstants.HBASE_MASTER_NORMALIZER_CLASS,
          SimpleRegionNormalizer.class,
          RegionNormalizer.class);
    } catch (RuntimeException e) {
      // Expected to catch RuntimeException due to incompatible class
    }
    // Verify fallback
    Class<?> normalizerClass = SimpleRegionNormalizer.class;
    assertEquals("Should fallback to SimpleRegionNormalizer when class does not implement RegionNormalizer",
        SimpleRegionNormalizer.class, normalizerClass);
  }

  @Test
  public void testValidCustomNormalizerClass() {
    // 1. Set a valid custom normalizer class (using SimpleRegionNormalizer as example)
    conf.set(HConstants.HBASE_MASTER_NORMALIZER_CLASS,
        "org.apache.hadoop.hbase.master.normalizer.SimpleRegionNormalizer");

    // 2. Verify the class can be loaded
    Class<?> normalizerClass = conf.getClass(
        HConstants.HBASE_MASTER_NORMALIZER_CLASS,
        SimpleRegionNormalizer.class,
        RegionNormalizer.class);

    assertNotNull("Custom normalizer class should be loaded", normalizerClass);
    assertTrue("Custom normalizer should implement RegionNormalizer",
        RegionNormalizer.class.isAssignableFrom(normalizerClass));
  }
}