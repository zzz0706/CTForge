package org.apache.hadoop.hbase.master.cleaner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertArrayEquals;

@Category({MasterTests.class, MediumTests.class})
public class LogCleanerPluginsConfigTest {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(LogCleanerPluginsConfigTest.class);

  @Test
  public void testDefaultLogCleanerChainIsLoadedWhenNoUserOverride() {
    // 1. Instantiate Configuration with default resources
    Configuration conf = new Configuration(false);
    conf.addResource("hbase-default.xml");

    // 2. Dynamic expected value calculation
    String[] defaultCleaners = conf.getStrings(HConstants.HBASE_MASTER_LOGCLEANER_PLUGINS,
        "org.apache.hadoop.hbase.master.cleaner.TimeToLiveLogCleaner,org.apache.hadoop.hbase.master.cleaner.TimeToLiveProcedureWALCleaner");
    String[] expected = new String[defaultCleaners.length + 1];
    System.arraycopy(defaultCleaners, 0, expected, 0, defaultCleaners.length);
    expected[expected.length - 1] = "org.apache.hadoop.hbase.replication.master.ReplicationLogCleaner";

    // 3. No external dependencies to mock for this test case

    // 4. Invoke the method under test
    HMaster.decorateMasterConfiguration(conf);

    // 5. Assertions and verification
    String[] actual = conf.getStrings(HConstants.HBASE_MASTER_LOGCLEANER_PLUGINS);
    assertArrayEquals(expected, actual);
  }
}