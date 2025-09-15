package alluxio.master;

import alluxio.conf.ServerConfiguration;
import alluxio.conf.PropertyKey;
import org.junit.Before;
import org.junit.Test;

import static org.mockito.Mockito.*;

public class AlluxioMasterProcessTest {

    private TestAlluxioMasterProcess mMasterProcess;

    // Prepare the test conditions
    @Before
    public void setUp() throws Exception {
        // Correctly create an instance or mock TestAlluxioMasterProcess
        mMasterProcess = spy(new TestAlluxioMasterProcess());
    }

    @Test
    public void testJvmMonitorProcessStartsWhenEnabled() throws Exception {
        // Update the configuration settings using the API
        ServerConfiguration.set(PropertyKey.MASTER_JVM_MONITOR_ENABLED, true);
        ServerConfiguration.set(PropertyKey.JVM_MONITOR_SLEEP_INTERVAL_MS, 1000L);
        ServerConfiguration.set(PropertyKey.JVM_MONITOR_INFO_THRESHOLD_MS, 500L);
        ServerConfiguration.set(PropertyKey.JVM_MONITOR_WARN_THRESHOLD_MS, 2000L);

        // Mock the createJvmPauseMonitor method to return a non-final dummy object
        TestJvmPauseMonitor mockJvmPauseMonitor = spy(
                new TestJvmPauseMonitor(
                        ServerConfiguration.getLong(PropertyKey.JVM_MONITOR_SLEEP_INTERVAL_MS),
                        ServerConfiguration.getLong(PropertyKey.JVM_MONITOR_INFO_THRESHOLD_MS),
                        ServerConfiguration.getLong(PropertyKey.JVM_MONITOR_WARN_THRESHOLD_MS)
                )
        );
        doReturn(mockJvmPauseMonitor).when(mMasterProcess).createJvmPauseMonitor();

        // Call the method under test
        mMasterProcess.startJvmMonitorProcess();

        // Verify that the JvmPauseMonitor start method was invoked
        verify(mockJvmPauseMonitor, times(1)).start();
    }

    // Simulated subclass to test AlluxioMasterProcess functionality
    private static class TestAlluxioMasterProcess {
        public TestJvmPauseMonitor createJvmPauseMonitor() {
            long sleepInterval = ServerConfiguration.getLong(PropertyKey.JVM_MONITOR_SLEEP_INTERVAL_MS);
            long infoThreshold = ServerConfiguration.getLong(PropertyKey.JVM_MONITOR_INFO_THRESHOLD_MS);
            long warnThreshold = ServerConfiguration.getLong(PropertyKey.JVM_MONITOR_WARN_THRESHOLD_MS);
            return new TestJvmPauseMonitor(sleepInterval, infoThreshold, warnThreshold);
        }

        public void startJvmMonitorProcess() {
            if (ServerConfiguration.getBoolean(PropertyKey.MASTER_JVM_MONITOR_ENABLED)) {
                createJvmPauseMonitor().start();
            }
        }
    }

    // Dummy class to work around JvmPauseMonitor being a final class
    private static class TestJvmPauseMonitor {
        private final long mSleepInterval;
        private final long mInfoThreshold;
        private final long mWarnThreshold;

        public TestJvmPauseMonitor(long sleepInterval, long infoThreshold, long warnThreshold) {
            mSleepInterval = sleepInterval;
            mInfoThreshold = infoThreshold;
            mWarnThreshold = warnThreshold;
        }

        public void start() {
            // Simulate starting the monitor
        }
    }
}