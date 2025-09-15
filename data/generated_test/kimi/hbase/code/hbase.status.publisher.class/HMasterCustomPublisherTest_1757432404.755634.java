package org.apache.hadoop.hbase.master;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.master.ClusterStatusPublisher.Publisher;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.JVMClusterUtil.MasterThread;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ MasterTests.class, SmallTests.class })
public class HMasterCustomPublisherTest {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(HMasterCustomPublisherTest.class);

  public static class DummyPublisher implements Publisher {
    public static volatile boolean instantiated = false;

    public DummyPublisher() {
      instantiated = true;
    }

    @Override
    public void publish(ClusterStatus status) {
      // no-op
    }

    @Override
    public void connect(Configuration conf) {
      // no-op
    }
  }

  private Configuration conf;
  private MasterThread masterThread;

  @Before
  public void setUp() {
    DummyPublisher.instantiated = false;
    conf = HBaseConfiguration.create();
    conf.setBoolean(HConstants.STATUS_PUBLISHED, true);
    conf.set(ClusterStatusPublisher.STATUS_PUBLISHER_CLASS, DummyPublisher.class.getName());
    // Disable cluster for unit test
    conf.setBoolean("hbase.testing.nocluster", true);
  }

  @After
  public void tearDown() throws Exception {
    if (masterThread != null) {
      masterThread.getMaster().stop("test teardown");
      masterThread.join();
    }
  }

  @Test
  public void testCustomPublisherClassIsInstantiatedWhenProvided() throws Exception {
    // 1. Configuration already prepared in setUp using hbase 2.2.2 API
    assertEquals(DummyPublisher.class.getName(),
        conf.get(ClusterStatusPublisher.STATUS_PUBLISHER_CLASS));

    // 2. Prepare test conditions: start a lightweight master
    masterThread = new MasterThread(new HMaster(conf), 0);
    masterThread.start();
    masterThread.getMaster().waitForServerOnline();

    // 3. Test code: verify DummyPublisher was instantiated
    assertTrue("DummyPublisher should have been instantiated",
        DummyPublisher.instantiated);

    // 4. Code after testing: handled in tearDown
  }
}