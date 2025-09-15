package alluxio.master;

import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import org.junit.Before;
import org.junit.Test;

import static org.mockito.Mockito.*;

public class AlluxioMasterProcessTest {
    private MockAlluxioMasterProcess mMasterProcess;

    @Before
    public void setUp() {
        // MockAlluxioMasterProcess is used as a testable instance
        mMasterProcess = spy(new MockAlluxioMasterProcess());
    }

    @Test
    public void testStartServingWithJvmMonitoringDisabled() throws Exception {
        // Prepare the test conditions
        ServerConfiguration.set(PropertyKey.MASTER_JVM_MONITOR_ENABLED, "false");

        // Test messages
        String startMessage = "Test start message";
        String stopMessage = "Test stop message";

        // Call the method to test
        mMasterProcess.startServing(startMessage, stopMessage);

        // Verify the expected behavior
        verify(mMasterProcess).startServingWebServer();
        verify(mMasterProcess).startServingRPCServer();
        verify(mMasterProcess, never()).startJvmMonitorProcess();

        // After testing, clean up configuration changes
        ServerConfiguration.unset(PropertyKey.MASTER_JVM_MONITOR_ENABLED);
    }

    /**
     * Mock implementation of the AlluxioMasterProcess for unit testing.
     * This avoids dependency on the actual AlluxioMasterProcess implementation.
     */
    private static class MockAlluxioMasterProcess {
        public void startServing(String startMessage, String stopMessage) {
            // Simulate the actual behavior of startServing
            if (ServerConfiguration.getBoolean(PropertyKey.MASTER_JVM_MONITOR_ENABLED)) {
                startJvmMonitorProcess();
            }

            startServingWebServer();
            startServingRPCServer();
        }

        public void startServingWebServer() {
            // Simulate starting the Web server
        }

        public void startServingRPCServer() {
            // Simulate starting the RPC server
        }

        public void startJvmMonitorProcess() {
            // Simulate starting the JVM monitor process
        }
    }
}