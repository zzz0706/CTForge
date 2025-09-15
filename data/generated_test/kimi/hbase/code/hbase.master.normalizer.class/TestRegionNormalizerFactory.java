package org.apache.hadoop.hbase.master.normalizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertEquals;

@Category({MasterTests.class, SmallTests.class})
public class TestRegionNormalizerFactory {

  @ClassRule
  public static final org.apache.hadoop.hbase.HBaseClassTestRule CLASS_RULE =
      org.apache.hadoop.hbase.HBaseClassTestRule.forClass(TestRegionNormalizerFactory.class);

  @Test
  public void testDefaultNormalizerIsInstantiatedWhenPropertyAbsent() {
    // 1. Create a fresh Configuration without touching the property
    Configuration conf = new Configuration();

    // 2. Read the default value dynamically
    Class<?> expectedClass = conf.getClass(
        HConstants.HBASE_MASTER_NORMALIZER_CLASS,
        SimpleRegionNormalizer.class,
        RegionNormalizer.class);

    // 3. Invoke the factory method
    RegionNormalizer normalizer = RegionNormalizerFactory.getRegionNormalizer(conf);

    // 4. Assert the actual instance type matches the expected default
    assertEquals(expectedClass, normalizer.getClass());
  }
}