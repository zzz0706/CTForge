package alluxio.conf;

import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import org.junit.Assert;
import org.junit.Test;

public class InstancedConfigurationTest {

    @Test
    public void validate_with_valid_heartbeat_interval() {
        // 1. Prepare the test conditions using the Alluxio 2.1.0 API to set configurations.
        InstancedConfiguration instancedConfiguration = InstancedConfiguration.defaults();

        // Set valid configuration values dynamically using the Alluxio API.
        instancedConfiguration.set(PropertyKey.MASTER_STANDBY_HEARTBEAT_INTERVAL, "100ms");
        instancedConfiguration.set(PropertyKey.MASTER_HEARTBEAT_TIMEOUT, "200ms");

        // Validate the configuration, ensuring it is applied correctly.
        Assert.assertEquals("100ms", instancedConfiguration.get(PropertyKey.MASTER_STANDBY_HEARTBEAT_INTERVAL));
        Assert.assertEquals("200ms", instancedConfiguration.get(PropertyKey.MASTER_HEARTBEAT_TIMEOUT));

        try {
            // 2. Call the validate method to test internal validation behavior.
            instancedConfiguration.validate();

            // Ensure the validate method completes without throwing any exceptions.
            Assert.assertTrue(true);
        } catch (IllegalArgumentException e) {
            // Fail the test if validate throws an exception for valid configuration.
            Assert.fail("Validation failed with valid configuration values: " + e.getMessage());
        }

        // 3. Ensure that the configurations are correctly propagated and used.
        long heartbeatIntervalMs = instancedConfiguration.getMs(PropertyKey.MASTER_STANDBY_HEARTBEAT_INTERVAL);
        long heartbeatTimeoutMs = instancedConfiguration.getMs(PropertyKey.MASTER_HEARTBEAT_TIMEOUT);

        // Ensure the interval is less than the timeout as required.
        Assert.assertTrue(heartbeatIntervalMs < heartbeatTimeoutMs);
    }

    @Test
    public void validate_master_heartbeat_thread_configuration() {
        // Preparing the configuration explicitly for validation in the context of master heartbeat threads.
        InstancedConfiguration instancedConfiguration = InstancedConfiguration.defaults();

        // Set required configuration values.
        instancedConfiguration.set(PropertyKey.MASTER_STANDBY_HEARTBEAT_INTERVAL, "100ms");
        instancedConfiguration.set(PropertyKey.MASTER_HEARTBEAT_TIMEOUT, "200ms");

        // Verify the interval and timeout are correctly set.
        Assert.assertEquals("100ms", instancedConfiguration.get(PropertyKey.MASTER_STANDBY_HEARTBEAT_INTERVAL));
        Assert.assertEquals("200ms", instancedConfiguration.get(PropertyKey.MASTER_HEARTBEAT_TIMEOUT));

        try {
            // Ensure proper validation.
            instancedConfiguration.validate();

            // Validate that heartbeat threads would perform properly based on the configuration.
            long heartbeatIntervalMs = instancedConfiguration.getMs(PropertyKey.MASTER_STANDBY_HEARTBEAT_INTERVAL);
            long heartbeatTimeoutMs = instancedConfiguration.getMs(PropertyKey.MASTER_HEARTBEAT_TIMEOUT);

            // Ensure the interval is properly less than the timeout.
            Assert.assertTrue(heartbeatIntervalMs < heartbeatTimeoutMs);

        } catch (Exception e) {
            // Fail the test in case of any misconfiguration.
            Assert.fail("Failed due to invalid configuration values: " + e.getMessage());
        }
    }
}