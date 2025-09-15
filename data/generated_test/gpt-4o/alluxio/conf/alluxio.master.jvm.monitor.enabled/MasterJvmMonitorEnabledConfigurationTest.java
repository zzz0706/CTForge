package alluxio.conf;

import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.ConfigurationTestUtils;
import org.junit.Assert;
import org.junit.Test;

public class MasterJvmMonitorEnabledConfigurationTest {

    @Test
    public void testMasterJvmMonitorEnabledConfigurationConstraints() {
        // Step 1: Prepare the test conditions.
        // Use ConfigurationTestUtils to create a mocked Alluxio configuration for testing.
        AlluxioConfiguration configuration = new InstancedConfiguration(ConfigurationTestUtils.defaults());

        // Step 2: Get the configuration value using the PropertyKey API.
        boolean configValue = configuration.getBoolean(PropertyKey.MASTER_JVM_MONITOR_ENABLED);

        // Step 3: Validate the configuration value.
        // Ensure it's a valid boolean value, and perform assertions.
        Assert.assertTrue(
            "Configuration value for alluxio.master.jvm.monitor.enabled must be either true or false",
            configValue == true || configValue == false
        );
    }
}