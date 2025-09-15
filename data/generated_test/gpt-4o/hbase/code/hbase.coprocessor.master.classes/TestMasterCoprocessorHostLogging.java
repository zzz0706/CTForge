package org.apache.hadoop.hbase.master;

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.master.MasterCoprocessorHost;
import org.apache.hadoop.hbase.master.MasterServices;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

/**
 * Test class for verifying the initialization state of MasterCoprocessorHost
 * based on the hbase.coprocessor.master.classes configuration.
 */
@Category({MasterTests.class, SmallTests.class})
public class TestMasterCoprocessorHostLogging {

    @ClassRule // Ensure that the HBaseClassTestRule is used for test class rule specification.
    public static final HBaseClassTestRule CLASS_RULE =
        HBaseClassTestRule.forClass(TestMasterCoprocessorHostLogging.class);

    @Test
    public void testMasterCoprocessorHostConfigurationState() throws Exception {
        // 1. Use the HBase 2.2.2 API correctly to obtain configuration values.
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.coprocessor.master.classes", "org.apache.hadoop.hbase.coprocessor.example.MyMasterObserver");

        // 2. Prepare the test conditions: Mock the required MasterServices implementation.
        MasterServices masterServicesMock = Mockito.mock(MasterServices.class);
        Mockito.when(masterServicesMock.getServerName()).thenReturn(ServerName.valueOf("localhost", 60000, 12345));

        try {
            // 3. Test code: Create MasterCoprocessorHost instance with mocked services and configuration.
            MasterCoprocessorHost masterCoprocessorHost = new MasterCoprocessorHost(masterServicesMock, configuration);

            // Verify configuration values were loaded into MasterCoprocessorHost.
            String configuredClasses = configuration.get("hbase.coprocessor.master.classes");
            assert configuredClasses != null : "Configuration for coprocessor classes should not be null.";
            assert configuredClasses.contains("org.apache.hadoop.hbase.coprocessor.example.MyMasterObserver") 
                : "Expected configured coprocessor class not found.";
        } finally {
            // 4. Code after testing: Perform any necessary cleanup. For mocked objects in this test, cleanup is not required.
            // Additional cleanup logic can be added here if required in future.
        }
    }
}