package org.apache.hadoop.hbase.procedure;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.procedure.MasterProcedureManagerHost;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertTrue;

/**
 * Test class for MasterProcedureManagerHost functionality.
 * It tests the behavior of the class with an empty configuration setup.
 */
@Category({MasterTests.class, SmallTests.class})
public class TestMasterProcedureManagerHost {
    @ClassRule // HBaseClassTestRule ClassRule
    public static final HBaseClassTestRule CLASS_RULE = HBaseClassTestRule.forClass(TestMasterProcedureManagerHost.class);

    @Test
    public void testLoadProceduresWithEmptyConfiguration() throws Exception {
        // Prepare the test conditions
        // 1. Create a basic Configuration object.
        Configuration conf = new Configuration();

        // 2. Ensure that the configuration key `hbase.procedure.master.classes` is unset (empty or null).
        conf.unset(MasterProcedureManagerHost.MASTER_PROCEDURE_CONF_KEY);
        String[] procedureClasses = conf.getStrings(MasterProcedureManagerHost.MASTER_PROCEDURE_CONF_KEY);

        // Ensure procedureClasses is null or empty as expected in the configuration.
        assertTrue("Procedure classes should be null or empty.", procedureClasses == null || procedureClasses.length == 0);

        // Test code
        // 1. Instantiate MasterProcedureManagerHost.
        // The constructor does not accept any arguments, so we do not pass `null` as originally attempted.
        MasterProcedureManagerHost procedureManagerHost = new MasterProcedureManagerHost();

        // 2. Execute the `loadProcedures` method with the provided configuration.
        procedureManagerHost.loadProcedures(conf);

        // 3. Verify post-test conditions
        // Ensure no procedures have been loaded into the procedureMgrMap.
        assertTrue("Procedure Manager Map should be empty.", procedureManagerHost.getProcedureManagers().isEmpty());

        // Code after testing
        // No specific resources to clean up; Configuration cleanup is handled automatically.
    }
}