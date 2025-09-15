package alluxio.client.file.options;

import alluxio.ClientContext;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class OutStreamOptionsTest {

    /**
     * Test to verify the `defaults` method in `OutStreamOptions` correctly initializes `mReplicationMin`
     * based on the configuration value of `alluxio.user.file.replication.min`.
     */
    @Test
    public void testDefaultsReplicationMin() {
        // 1. Prepare the test conditions.
        // Create an InstancedConfiguration instance with the default AlluxioConfiguration.
        InstancedConfiguration alluxioConf = InstancedConfiguration.defaults();
        int expectedReplicationMin = 3; // Example value for testing.
        alluxioConf.set(PropertyKey.USER_FILE_REPLICATION_MIN, Integer.toString(expectedReplicationMin));

        // Create a ClientContext with the initialized configuration.
        ClientContext context = ClientContext.create(alluxioConf);

        // 2. Test code.
        // Invoke the `defaults` method in `OutStreamOptions` and collect the returned instance.
        OutStreamOptions options = OutStreamOptions.defaults(context);

        // 3. Code after testing.
        // Assert the value of `mReplicationMin` in the returned `OutStreamOptions` instance.
        assertEquals(expectedReplicationMin, options.getReplicationMin());
    }
}