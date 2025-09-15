package org.apache.hadoop.hbase.regionserver.compactions;

import static org.junit.Assert.assertTrue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.apache.hadoop.hbase.HBaseClassTestRule;

@Category(SmallTests.class)
public class TestCompactionMaxConfigValidation {

  private static Configuration conf;

  @ClassRule
  public static final HBaseClassTestRule classRule =
      HBaseClassTestRule.forClass(TestCompactionMaxConfigValidation.class);

  @BeforeClass
  public static void setUp() {
    // 1. You need to use the hbase 2.2.2 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    conf = HBaseConfiguration.create();
  }

  @Test
  public void testHBaseHstoreCompactionMaxIsPositiveInt() {
    // 2. Prepare the test conditions.
    // 3. Test code.
    int maxFiles = conf.getInt(CompactionConfiguration.HBASE_HSTORE_COMPACTION_MAX_KEY, 10);
    assertTrue("hbase.hstore.compaction.max must be a positive integer",
               maxFiles > 0);
    // 4. Code after testing.
  }
}