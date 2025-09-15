package org.apache.hadoop.hbase.coprocessor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyZeroInteractions;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({MasterTests.class, SmallTests.class})
public class MasterCoprocessorHostTest {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(MasterCoprocessorHostTest.class);

  @Test
  public void testDefaultEmptyList_NoCoprocessorsLoaded() throws Exception {
    // 1. You need to use the hbase 2.2.2 API correctly to obtain configuration values,
    //    instead of hardcoding the configuration values.
    Configuration conf = new Configuration(); // no explicit set(...) => default/empty

    // 2. Prepare the test conditions.
    String[] expectedClasses = conf.getStrings(CoprocessorHost.MASTER_COPROCESSOR_CONF_KEY);
    int expectedCount = (expectedClasses == null) ? 0 : expectedClasses.length;

    MasterServices services = mock(MasterServices.class);

    // 3. Test code.
    org.apache.hadoop.hbase.master.MasterCoprocessorHost host =
        new org.apache.hadoop.hbase.master.MasterCoprocessorHost(services, conf);

    // 4. Code after testing.
    assertEquals("No coprocessors should be loaded", expectedCount, host.getCoprocessors().size());
    verifyZeroInteractions(services);
  }
}