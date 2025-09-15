package org.apache.hadoop.hbase.procedure;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({MasterTests.class, SmallTests.class})
public class TestMasterProcedureManagerHost {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestMasterProcedureManagerHost.class);

  @Test
  public void loadUserProcedures_emptyConfig_noManagersLoaded() throws Exception {
    // 1. Configuration as input
    Configuration conf = new Configuration();

    // 2. Dynamic expected value calculation
    String[] configuredClasses = conf.getStrings(
        MasterProcedureManagerHost.MASTER_PROCEDURE_CONF_KEY, new String[0]);
    long expectedCount = configuredClasses.length;

    // 3. Mock/stub external dependencies
    // (none required for this test; the method only uses conf.getStrings)

    // 4. Invoke the method under test
    MasterProcedureManagerHost host = new MasterProcedureManagerHost();
    host.loadProcedures(conf);

    // 5. Assertions and verification
    assertEquals(expectedCount, host.getProcedureManagers().size());
    assertTrue(host.getProcedureManagers().isEmpty());
  }
}