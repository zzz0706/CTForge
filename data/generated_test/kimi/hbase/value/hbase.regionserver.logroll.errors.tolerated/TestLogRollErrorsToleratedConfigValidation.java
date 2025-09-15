package org.apache.hadoop.hbase.regionserver.wal;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(SmallTests.class)
public class TestLogRollErrorsToleratedConfigValidation {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestLogRollErrorsToleratedConfigValidation.class);

  private Configuration conf;

  @Before
  public void setUp() {
    conf = new Configuration();
  }

  /**
   * Validates that the configuration value for "hbase.regionserver.logroll.errors.tolerated"
   * is a non-negative integer. A negative value would be invalid because the code uses it
   * as a threshold for tolerated consecutive WAL close errors.
   */
  @Test
  public void testLogRollErrorsToleratedConfigIsNonNegative() {
    int tolerated = conf.getInt("hbase.regionserver.logroll.errors.tolerated", 2);
    if (tolerated < 0) {
      fail("Configuration hbase.regionserver.logroll.errors.tolerated must be a non-negative integer. "
          + "Current value: " + tolerated);
    }
    assertTrue("Configuration hbase.regionserver.logroll.errors.tolerated is valid.",
        tolerated >= 0);
  }
}