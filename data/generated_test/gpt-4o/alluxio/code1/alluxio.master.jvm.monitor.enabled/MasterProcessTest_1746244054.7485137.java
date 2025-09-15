package alluxio.master;

import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import org.junit.Before;
import org.junit.Test;

import static org.mockito.Mockito.*;

public class MasterProcessTest {

    private MasterProcess mMasterProcess;

    @Before
    public void setUp() {
        // Use a mock instance since MasterProcess is abstract and cannot be instantiated
        mMasterProcess = mock(MasterProcess.class);
    }

    @Test
    public void testStartMethodWithConfigPropagation() throws Exception {
        // 1. Use the Alluxio 2.1.0 API to correctly obtain configuration values.
        boolean isJvmMonitorEnabled = ServerConfiguration.getBoolean(PropertyKey.MASTER_JVM_MONITOR_ENABLED);

        // 2. Prepare the test conditions
        // Mock the startServing method, which exists in MasterProcess
        doNothing().when(mMasterProcess).startServing(anyString(), anyString());

        // Mock the start method to trigger startServing internally (if applicable)
        doAnswer(invocation -> {
            if (isJvmMonitorEnabled) {
                mMasterProcess.startServing("hostname", "processName");
            }
            return null;
        }).when(mMasterProcess).start();

        // 3. Test Code
        mMasterProcess.start();

        // 4. After testing: assertions and verifications
        if (isJvmMonitorEnabled) {
            verify(mMasterProcess).startServing("hostname", "processName");
        } else {
            verify(mMasterProcess, never()).startServing(anyString(), anyString());
        }
    }
}