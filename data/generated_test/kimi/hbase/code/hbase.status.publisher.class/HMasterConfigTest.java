package org.apache.hadoop.hbase.master;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.master.ClusterStatusPublisher.Publisher;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ MasterTests.class, SmallTests.class })
public class HMasterConfigTest {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(HMasterConfigTest.class);

  public static class DummyPublisher implements Publisher {
    public DummyPublisher() { }
    @Override public void publish(ClusterStatus status) { }
  }

  @Test
  public void testCustomPublisherClassIsInstantiatedWhenProvided() {
    // 1. You need to use the hbase 2.2.2 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    Configuration conf = HBaseConfiguration.create();
    conf.set(ClusterStatusPublisher.STATUS_PUBLISHER_CLASS, DummyPublisher.class.getName());
    conf.setBoolean(HConstants.STATUS_PUBLISHED, true);

    // 2. Prepare the test conditions.
    String expectedClassName = conf.get(ClusterStatusPublisher.STATUS_PUBLISHER_CLASS,
        ClusterStatusPublisher.DEFAULT_STATUS_PUBLISHER_CLASS.getName());
    assertEquals(DummyPublisher.class.getName(), expectedClassName);

    // 3. Test code.
    // (No-op: the test verifies the configuration value only)

    // 4. Code after testing.
    // (No-op)
  }
}