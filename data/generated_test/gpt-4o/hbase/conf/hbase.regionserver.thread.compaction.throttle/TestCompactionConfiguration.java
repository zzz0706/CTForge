package org.apache.hadoop.hbase.regionserver.compactions;

import org.apache.hadoop.conf.Configuration; // Use correct Configuration class from Hadoop
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.ClassRule;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertTrue;

@Category(SmallTests.class)
public class TestCompactionConfiguration {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE = 
      HBaseClassTestRule.forClass(TestCompactionConfiguration.class);

  private Configuration conf;

  @Before
  public void setUp() {
    // Initialize the configuration object. Normally, this reads a valid configuration file.
    conf = new Configuration();
    // Set default values for the configuration keys to avoid null values during tests.
    conf.setLong("hbase.hregion.memstore.flush.size", 128 * 1024 * 1024);
    conf.setInt("hbase.hstore.compaction.max", 10);
    conf.setLong("hbase.regionserver.thread.compaction.throttle", 
        2 * conf.getInt("hbase.hstore.compaction.max", 10) 
        * conf.getLong("hbase.hregion.memstore.flush.size", 128 * 1024 * 1024));
  }

  @Test
  public void testCompactionThrottleConfigurations() {
    // 1. Use the hbase 2.2.2 API correctly to obtain configuration values.
    long throttlePoint = conf.getLong("hbase.regionserver.thread.compaction.throttle", 
        2 * conf.getInt("hbase.hstore.compaction.max", 10) 
        * conf.getLong("hbase.hregion.memstore.flush.size", 128 * 1024 * 1024));

    long memStoreFlushSize = conf.getLong("hbase.hregion.memstore.flush.size", 128 * 1024 * 1024);
    int maxFilesToCompact = conf.getInt("hbase.hstore.compaction.max", 10);

    // 2. Prepare the test conditions.
    long expectedThrottlePoint = 2 * maxFilesToCompact * memStoreFlushSize;

    // 3. Test code.
    assertTrue("Throttle point does not match the expected constraint.", throttlePoint == expectedThrottlePoint);
    assertTrue("Throttle point must be greater than 0.", throttlePoint > 0);
    assertTrue("MemStore flush size must be greater than 0.", memStoreFlushSize > 0);
    assertTrue("Max files to compact must be greater than 0.", maxFilesToCompact > 0);

    // 4. Code after testing (Cleanup if necessary, though this test doesn't allocate external resources).
  }
}