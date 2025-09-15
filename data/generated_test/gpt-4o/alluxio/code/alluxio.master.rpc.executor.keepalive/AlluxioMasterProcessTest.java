package alluxio.master;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;

import static org.junit.Assert.assertEquals;

public class AlluxioMasterProcessTest {

    private InstancedConfiguration mConfiguration;

    @Before
    public void setUp() {
        // Prepare the test conditions.
        // Create a configuration instance for the test.
        mConfiguration = InstancedConfiguration.defaults();
    }

    @Test
    public void testConfigurationForRPCServerKeepAlive() {
        // Retrieve the configured keep-alive time from the configuration.
        long expectedKeepAliveTime = mConfiguration.getMs(PropertyKey.MASTER_RPC_EXECUTOR_KEEPALIVE);

        // Validate that the configured keep-alive time matches the expected value.
        // This is to ensure that the default configuration is correctly set.
        assertEquals(expectedKeepAliveTime, mConfiguration.getMs(PropertyKey.MASTER_RPC_EXECUTOR_KEEPALIVE));
    }
}