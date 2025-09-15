package org.apache.hadoop.hbase.procedure;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Collection;

import static org.junit.Assert.assertEquals;

@Category({MasterTests.class, SmallTests.class})
public class TestMasterProcedureManagerHost {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestMasterProcedureManagerHost.class);

  public static class ValidProc extends MasterProcedureManager {
    @Override
    public String getProcedureSignature() {
      return "ValidProc";
    }
  }

  @Test
  public void loadUserProcedures_withWhitespaceOnlyNames_skipsEmptyEntries() {
    // 1. Create a fresh Configuration instance
    Configuration conf = new Configuration();

    // 2. Set the configuration with extra commas and whitespace
    conf.set(ProcedureManagerHost.MASTER_PROCEDURE_CONF_KEY,
             "  , " + ValidProc.class.getName() + " ,  ");

    // 3. Instantiate the host and load procedures
    MasterProcedureManagerHost host = new MasterProcedureManagerHost();
    host.loadProcedures(conf);

    // 4. Verify the loaded managers
    Collection<MasterProcedureManager> managers = host.getProcedureManagers();
    assertEquals("Only one manager should be loaded", 1, managers.size());
    assertEquals("Loaded manager should be ValidProc",
                 ValidProc.class, managers.iterator().next().getClass());
  }
}