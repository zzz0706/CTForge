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
    // 1. Obtain configuration via the HBase 2.2.2 API – do NOT hard-code values
    Configuration conf = new Configuration();

    // 2. Ensure the key is absent
    conf.unset(MasterProcedureManagerHost.MASTER_PROCEDURE_CONF_KEY);

    // 3. Execute the method under test
    MasterProcedureManagerHost host = new MasterProcedureManagerHost();
    host.loadProcedures(conf);

    // 4. Post-test verification
    assertTrue("Expected no managers when key is unset",
               host.getProcedureManagers().isEmpty());
    assertEquals("Expected size 0", 0, host.getProcedureManagers().size());
  }
}