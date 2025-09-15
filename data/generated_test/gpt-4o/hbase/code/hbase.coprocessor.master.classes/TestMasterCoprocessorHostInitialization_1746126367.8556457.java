package org.apache.hadoop.hbase.master;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.master.MasterCoprocessorHost;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.master.MasterServices;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

@Category({MasterTests.class, SmallTests.class})
public class TestMasterCoprocessorHostInitialization {

    @ClassRule
    public static final HBaseClassTestRule CLASS_RULE = HBaseClassTestRule.forClass(TestMasterCoprocessorHostInitialization.class);

    @Test
    public void test_MasterCoprocessorHost_initialization_with_empty_configuration() throws Exception {
        // Prepare the test conditions.
        // Create a Configuration object without setting 'hbase.coprocessor.master.classes'.
        Configuration conf = new Configuration();

        // Instantiate the MasterServices mock object.
        MasterServices mockMasterServices = mock(MasterServices.class);

        // Test code: Create an instance of MasterCoprocessorHost using the empty Configuration and MasterServices objects.
        MasterCoprocessorHost masterCoprocessorHost = new MasterCoprocessorHost(mockMasterServices, conf);

        // Verify that no coprocessors are loaded and the host remains functional.
        assertNotNull("MasterCoprocessorHost should be successfully initialized.", masterCoprocessorHost);
        assertEquals("No coprocessors should be loaded when 'hbase.coprocessor.master.classes' is absent or empty.", 0, masterCoprocessorHost.getCoprocessors().size());
    }
}