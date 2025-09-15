package alluxio.test;

import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.AlluxioProperties;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.ClientContext;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class ConfigurationTest {
    private AlluxioProperties mProperties;
    private AlluxioConfiguration mConfiguration;

    // Prepare the test environment
    @Before
    public void setUp() {
        // Create a new AlluxioProperties instance for testing
        mProperties = new AlluxioProperties();

        // Set properties for testing
        mProperties.set(PropertyKey.MASTER_HOSTNAME, "localhost");
        mProperties.set(PropertyKey.MASTER_RPC_PORT, "19998");

        // Instantiate configuration with the properties
        mConfiguration = new InstancedConfiguration(mProperties);
    }

    @Test
    public void testMasterHostnameConfiguration() {
        // Test for correctly fetching the configuration value of MASTER_HOSTNAME
        String hostname = mConfiguration.get(PropertyKey.MASTER_HOSTNAME);
        Assert.assertEquals("localhost", hostname);
    }

    @Test
    public void testMasterPortConfiguration() {
        // Test for correctly fetching the configuration value of MASTER_RPC_PORT
        String port = mConfiguration.get(PropertyKey.MASTER_RPC_PORT);
        Assert.assertEquals("19998", port);
    }

    @Test
    public void testClientContextInitialization() {
        // Test if the ClientContext is initialized correctly based on configuration values
        ClientContext context = ClientContext.create(mConfiguration);
        Assert.assertNotNull(context);
        Assert.assertEquals("localhost", context.getClusterConf().get(PropertyKey.MASTER_HOSTNAME));
        Assert.assertEquals("19998", context.getClusterConf().get(PropertyKey.MASTER_RPC_PORT));
    }
}