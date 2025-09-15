package alluxio.conf;

import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import org.junit.Test;

import static org.junit.Assert.fail;

public class InstancedConfigurationTest {
    @Test
    public void validateHeartbeatIntervalWithMultipleChecksTest() {
        // 1. Using proper Alluxio 2.1.0 API to obtain and modify configuration values without referencing non-existing classes.
        // Prepare the test conditions
        InstancedConfiguration instanceConfig = InstancedConfiguration.defaults();

        // 2. Setting valid configuration values using the API
        instanceConfig.set(PropertyKey.MASTER_STANDBY_HEARTBEAT_INTERVAL, "1000ms");
        instanceConfig.set(PropertyKey.MASTER_HEARTBEAT_TIMEOUT, "2000ms");
        instanceConfig.set(PropertyKey.MASTER_WORKER_CONNECT_WAIT_TIME, "5000ms");
        instanceConfig.set(PropertyKey.USER_RPC_RETRY_MAX_SLEEP_MS, "2000ms");
        instanceConfig.set(PropertyKey.CONF_VALIDATION_ENABLED, "true");

        // 3. Ensure the configurations are correctly passed.
        try {
            // Validate the configurations
            instanceConfig.validate();
        } catch (Exception e) {
            // If validation fails, handle the exception correctly.
            fail("Validation failed for configurations: " + e.getMessage());
        }
    }
}