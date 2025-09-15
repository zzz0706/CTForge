package alluxio.master.meta;

import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import org.junit.Assert;
import org.junit.Test;

public class DefaultMetaMasterTest {

    // Test case: heartbeatTimeoutConfigurationValidation
    // Objective: Verify that the `checkHeartbeatTimeout` method properly validates configuration values for heartbeat intervals and timeouts.
    @Test
    public void testHeartbeatTimeoutConfigurationValidation() {
        // 1. Prepare the test conditions
        // Get the heartbeat interval and timeout values using the API.
        long heartbeatInterval = ServerConfiguration.getMs(PropertyKey.MASTER_STANDBY_HEARTBEAT_INTERVAL);
        long heartbeatTimeout = ServerConfiguration.getMs(PropertyKey.MASTER_HEARTBEAT_TIMEOUT);

        // 2. Test code
        // Validate that the interval value obtained is less than the timeout value.
        boolean isValid = heartbeatInterval < heartbeatTimeout;

        // 3. Code after testing
        // Assert that the configuration is valid
        Assert.assertTrue(
            String.format("Heartbeat interval (%s ms) must be less than heartbeat timeout (%s ms).",
                heartbeatInterval, heartbeatTimeout),
            isValid
        );
    }
}