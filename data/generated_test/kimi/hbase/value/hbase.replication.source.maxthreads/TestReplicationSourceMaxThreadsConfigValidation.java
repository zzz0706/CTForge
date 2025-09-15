package org.apache.hadoop.hbase.replication.regionserver;

import static org.junit.Assert.assertTrue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.testclassification.ReplicationTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ReplicationTests.class, SmallTests.class})
public class TestReplicationSourceMaxThreadsConfigValidation {

  @ClassRule
  public static final org.apache.hadoop.hbase.HBaseClassTestRule CLASS_RULE =
      org.apache.hadoop.hbase.HBaseClassTestRule.forClass(TestReplicationSourceMaxThreadsConfigValidation.class);

  @Test
  public void testMaxThreadsValid() {
    Configuration conf = HBaseConfiguration.create();
    // 1. Obtain value from configuration file, no hard-coding
    int maxThreads = conf.getInt(HConstants.REPLICATION_SOURCE_MAXTHREADS_KEY,
                                 HConstants.REPLICATION_SOURCE_MAXTHREADS_DEFAULT);

    // 2. Validate constraints: must be a positive integer
    assertTrue("hbase.replication.source.maxthreads must be > 0", maxThreads > 0);

    // 3. Validate type: ensure it is an integer
    try {
      Integer.parseInt(conf.get(HConstants.REPLICATION_SOURCE_MAXTHREADS_KEY,
                                String.valueOf(HConstants.REPLICATION_SOURCE_MAXTHREADS_DEFAULT)));
    } catch (NumberFormatException nfe) {
      assertTrue("hbase.replication.source.maxthreads must be an integer", false);
    }
  }
}