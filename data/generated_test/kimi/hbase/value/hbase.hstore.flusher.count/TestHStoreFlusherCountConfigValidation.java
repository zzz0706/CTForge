package org.apache.hadoop.hbase.regionserver;

import static org.junit.Assert.assertTrue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({RegionServerTests.class, SmallTests.class})
public class TestHStoreFlusherCountConfigValidation {

  private static Configuration conf;

  @ClassRule
  public static final org.apache.hadoop.hbase.HBaseClassTestRule CLASS_RULE =
      org.apache.hadoop.hbase.HBaseClassTestRule.forClass(TestHStoreFlusherCountConfigValidation.class);

  @BeforeClass
  public static void setUp() {
    // Load configuration from the classpath (hbase-site.xml, hbase-default.xml, etc.)
    conf = HBaseConfiguration.create();
  }

  @Test
  public void testHStoreFlusherCountIsPositiveInteger() {
    int flusherCount = conf.getInt("hbase.hstore.flusher.count", 2);
    assertTrue("hbase.hstore.flusher.count must be a positive integer, but was: " + flusherCount,
               flusherCount > 0);
  }
}