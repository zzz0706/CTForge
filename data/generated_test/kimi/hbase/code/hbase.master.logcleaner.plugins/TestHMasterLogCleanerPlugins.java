package org.apache.hadoop.hbase.master;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertArrayEquals;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.master.cleaner.TimeToLiveLogCleaner;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.HBaseClassTestRule;

@Category({MasterTests.class, SmallTests.class})
public class TestHMasterLogCleanerPlugins {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestHMasterLogCleanerPlugins.class);

  @Test
  public void testReplicationLogCleanerIsAppendedOnlyOnceWhenAlreadyPresent() {
    // 1. Configuration as Input
    Configuration conf = new Configuration(false);

    // 2. Prepare the test conditions
    String original = TimeToLiveLogCleaner.class.getName() + ","
                    + "org.apache.hadoop.hbase.replication.master.ReplicationLogCleaner";
    conf.set(HConstants.HBASE_MASTER_LOGCLEANER_PLUGINS, original);

    // 3. Invoke the method under test
    HMaster.decorateMasterConfiguration(conf);

    // 4. Assertions and verification
    String[] actual = conf.getStrings(HConstants.HBASE_MASTER_LOGCLEANER_PLUGINS);
    int count = 0;
    for (String cls : actual) {
      if ("org.apache.hadoop.hbase.replication.master.ReplicationLogCleaner".equals(cls)) {
        count++;
      }
    }
    assertEquals("ReplicationLogCleaner should appear exactly once", 1, count);

    // After decorateMasterConfiguration, ReplicationLogCleaner is appended only once
    // when it is already present, so the array length should be 2.
    String[] expected = new String[]{
        TimeToLiveLogCleaner.class.getName(),
        "org.apache.hadoop.hbase.replication.master.ReplicationLogCleaner"
    };
    assertArrayEquals("Order should be preserved", expected, actual);
  }
}