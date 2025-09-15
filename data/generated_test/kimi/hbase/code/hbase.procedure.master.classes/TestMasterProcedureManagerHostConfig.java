package org.apache.hadoop.hbase.procedure;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Collection;

import static org.junit.Assert.*;

@Category({MasterTests.class, SmallTests.class})
public class TestMasterProcedureManagerHostConfig {

    @ClassRule
    public static final org.apache.hadoop.hbase.HBaseClassTestRule CLASS_RULE =
            org.apache.hadoop.hbase.HBaseClassTestRule.forClass(TestMasterProcedureManagerHostConfig.class);

    // Dummy implementation only used for the test
    public static class TestMasterProcedureManager extends MasterProcedureManager {
        @Override
        public void start() {}
        @Override
        public void stop() {}
        @Override
        public boolean isRunning() { return false; }
        @Override
        public String getProcedureSignature() { return "TestMasterProcedureManager"; }
    }

    @Test
    public void loadUserProcedures_withValidClassName_instantiatesManager() {
        // 1. Instantiate Configuration
        Configuration conf = HBaseConfiguration.create();

        // 2. Dynamic expected value: read the key from configuration (empty default)
        String[] expectedClasses = conf.getStrings(
                ProcedureManagerHost.MASTER_PROCEDURE_CONF_KEY,
                new String[]{"com.example.TestMasterProcedureManager"});

        // 3. Prepare test conditions: set the configuration with our test class
        conf.set(ProcedureManagerHost.MASTER_PROCEDURE_CONF_KEY,
                 "org.apache.hadoop.hbase.procedure.TestMasterProcedureManagerHostConfig$TestMasterProcedureManager");

        // 4. Invoke the method under test
        MasterProcedureManagerHost host = new MasterProcedureManagerHost();
        host.loadProcedures(conf);

        // 5. Assertions
        assertEquals("Exactly one manager should be loaded",
                     1, host.getProcedureManagers().size());
        Collection<MasterProcedureManager> managers = host.getProcedureManagers();
        assertTrue("Manager should be instance of TestMasterProcedureManager",
                   managers.iterator().next() instanceof TestMasterProcedureManager);
    }
}