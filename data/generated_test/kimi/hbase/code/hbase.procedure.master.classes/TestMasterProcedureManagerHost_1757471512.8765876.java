package org.apache.hadoop.hbase.procedure;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertEquals;

@Category({MasterTests.class, SmallTests.class})
public class TestMasterProcedureManagerHost {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestMasterProcedureManagerHost.class);

  @Test
  public void loadUserProcedures_withInvalidClassName_logsWarning() {
    // 1. Obtain a fresh Configuration instance
    Configuration conf = new Configuration();

    // 2. Set the invalid class name using the official constant
    conf.set(ProcedureManagerHost.MASTER_PROCEDURE_CONF_KEY,
             "com.example.NonExistentManager");

    // 3. Instantiate MasterProcedureManagerHost and trigger loadProcedures
    MasterProcedureManagerHost host = new MasterProcedureManagerHost();
    host.loadProcedures(conf);

    // 4. Verify that no procedure managers were loaded
    assertEquals(0, host.getProcedureManagers().size());
  }
}