package org.apache.hadoop.hbase.snapshot;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertTrue;

@Category({SmallTests.class})
public class TestSnapshotMasterTimeoutConfigValidation {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestSnapshotMasterTimeoutConfigValidation.class);

  /**
   * Ensure that the value supplied for "hbase.snapshot.master.timeout.millis"
   * is a positive long.  The configuration is read from the normal configuration
   * files (hbase-site.xml, hbase-default.xml, etc.) – no value is set
   * programmatically inside the test.
   */
  @Test
  public void testMasterSnapshotTimeoutIsPositiveLong() {
    Configuration conf = new Configuration(false);
    // Loads the real runtime configuration (hbase-default.xml, hbase-site.xml, …)
    conf.addResource("hbase-default.xml");
    conf.addResource("hbase-site.xml");

    long timeout = conf.getLong(SnapshotDescriptionUtils.MASTER_SNAPSHOT_TIMEOUT_MILLIS,
                                SnapshotDescriptionUtils.DEFAULT_MAX_WAIT_TIME);

    assertTrue("hbase.snapshot.master.timeout.millis must be a positive long",
               timeout > 0);
  }
}