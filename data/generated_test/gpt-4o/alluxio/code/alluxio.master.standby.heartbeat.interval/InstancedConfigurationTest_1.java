package alluxio.conf;

import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import org.junit.Assert;
import org.junit.Test;

public class InstancedConfigurationTest {
    @Test
    public void validate_with_valid_heartbeat_interval() {
        // Prepare the test conditions.
        InstancedConfiguration instancedConfiguration = InstancedConfiguration.defaults();
        instancedConfiguration.set(PropertyKey.MASTER_STANDBY_HEARTBEAT_INTERVAL, "1000ms");
        instancedConfiguration.set(PropertyKey.MASTER_HEARTBEAT_TIMEOUT, "2000ms");

        try {
            // Test code: Call validate method.
            instancedConfiguration.validate();

            // Ensure the validate method completes without throwing any exceptions.
            Assert.assertTrue(true);
        } catch (IllegalArgumentException e) {
            // Failing this test if an exception was thrown.
            Assert.fail("Validation failed when valid configuration values were provided: " + e.getMessage());
        }
    }
}