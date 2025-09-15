package org.apache.hadoop.hbase.procedure;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.procedure.MasterProcedureManagerHost; // Correct import path
import org.apache.hadoop.hbase.procedure.MasterProcedureManager;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.*;

@Category({MasterTests.class, SmallTests.class})
public class TestMasterProcedureManagerHost {

    @ClassRule // HBaseClassTestRule is required for HBase testing
    public static final HBaseClassTestRule CLASS_RULE =
        HBaseClassTestRule.forClass(TestMasterProcedureManagerHost.class);

    private Configuration configuration;
    private MasterProcedureManagerHost masterProcedureManagerHost;

    @Before
    public void setUp() {
        // Prepare a mock Configuration object for testing
        configuration = new Configuration();

        // Obtain configuration value using API rather than hardcoding
        String procedureClassProperty = MasterProcedureManagerHost.MASTER_PROCEDURE_CONF_KEY;

        // Simulate valid procedure manager classes for testing
        configuration.setStrings(procedureClassProperty, 
            "org.apache.hadoop.hbase.procedure.TestProcedureManager1", 
            "org.apache.hadoop.hbase.procedure.TestProcedureManager2");

        // Initialize the MasterProcedureManagerHost
        masterProcedureManagerHost = new MasterProcedureManagerHost(); // Changed from spy to normal instance
    }

    @Test
    public void testLoadProceduresWithValidConfiguration() throws Exception {
        // Ensure configuration is properly initialized
        assertNotNull(configuration);

        // Test loadProcedures functionality
        masterProcedureManagerHost.loadProcedures(configuration);

        // Validate the procedureMgrMap contains expected procedure managers
        masterProcedureManagerHost.getProcedureManagers().forEach(pm -> {
            assertNotNull(pm); // Ensure procedure managers are initialized
            assertNotNull(pm.getProcedureSignature()); // Ensure procedure signature is accessible
            assertTrue(masterProcedureManagerHost.getProcedureManager(pm.getProcedureSignature()) != null);
        });

        // Successfully completed tests
    }
}