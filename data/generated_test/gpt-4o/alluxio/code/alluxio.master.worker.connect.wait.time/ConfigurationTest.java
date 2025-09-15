package alluxio.test;

import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import org.junit.Assert;
import org.junit.Before;
import org.junit.After;
import org.junit.Test;

public class ConfigurationTest {
    private InstancedConfiguration mConfiguration;

    @Before
    public void setUp() {
        // Prepare the test conditions: Create an instanced configuration
        mConfiguration = InstancedConfiguration.defaults();
    }

    @Test
    public void testPropertyConfiguration() {
        // Prepare test by setting a property value
        mConfiguration.set(PropertyKey.MASTER_HOSTNAME, "localhost");

        // Validate the configuration value using Alluxio API
        String masterHostName = mConfiguration.get(PropertyKey.MASTER_HOSTNAME);

        // Assert that the property value has been correctly set and retrieved
        Assert.assertEquals("localhost", masterHostName);
    }

    @After
    public void tearDown() {
        // Clean up after the test: Reset the configuration by clearing all properties
        mConfiguration = InstancedConfiguration.defaults(); // Reinitialize to default settings
    }
}