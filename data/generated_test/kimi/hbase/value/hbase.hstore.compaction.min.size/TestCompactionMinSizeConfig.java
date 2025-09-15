package org.apache.hadoop.hbase.regionserver.compactions;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertTrue;

@Category(SmallTests.class)
public class TestCompactionMinSizeConfig {

  private Configuration conf;

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestCompactionMinSizeConfig.class);

  @Before
  public void setUp() {
    conf = HBaseConfiguration.create();
  }

  @After
  public void tearDown() {
    conf.clear();
  }

  @Test
  public void testCompactionMinSizeValid() {
    long minCompactSize = conf.getLong("hbase.hstore.compaction.min.size", 134217728L);
    assertTrue("hbase.hstore.compaction.min.size must be a positive long value", minCompactSize > 0);
  }
}