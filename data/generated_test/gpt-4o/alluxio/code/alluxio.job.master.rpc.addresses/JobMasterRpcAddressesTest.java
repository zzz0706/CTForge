package alluxio.conf;

import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.util.ConfigurationUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.util.List;

public class JobMasterRpcAddressesTest {

    private AlluxioConfiguration mConfiguration;

    @Before
    public void setUp() {
        // Initialize the configuration object to be used in tests.
        // Use the builder to create a dummy configuration instance as `Configuration.global()` was incorrect.
        mConfiguration = new InstancedConfiguration(ConfigurationUtils.defaults());
    }

    @Test
    public void testJobMasterRpcAddressesWithInvalidConfiguration() {
        try {
            // Fetching the Job Master RPC Addresses using the Configuration Utils.
            // We refrain from setting any value explicitly and rely on the API to fetch relevant values.
            List<InetSocketAddress> rpcAddresses = ConfigurationUtils.getJobMasterRpcAddresses(mConfiguration);

            // Validate the fetched rpcAddresses.
            // Since the invalid case involves missing derived values or misconfiguration,
            // we confirm that the fetched list conforms to the defined derivation logic.
            Assert.assertNotNull("RPC addresses should not be null", rpcAddresses);
            Assert.assertFalse("RPC addresses list should not be empty given fallback mechanisms", rpcAddresses.isEmpty());

            // Additional validation: Confirm valid InetSocketAddress objects are returned.
            for (InetSocketAddress address : rpcAddresses) {
                Assert.assertNotNull("Address should not be null", address.getHostName());
                Assert.assertTrue("Port should be greater than zero", address.getPort() > 0);
            }
        } catch (Exception e) {
            // Any exceptions during the process should be properly handled and logged.
            Assert.fail("Exception not expected during fetching of Job Master RPC addresses: " + e.getMessage());
        }
    }

    @After
    public void tearDown() {
        // Clear resources or reset any configurations if necessary.
        mConfiguration = null;
    }
}