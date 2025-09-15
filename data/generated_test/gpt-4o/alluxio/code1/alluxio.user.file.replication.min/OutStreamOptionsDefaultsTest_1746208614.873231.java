package alluxio.client.file.options;

import alluxio.ClientContext;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.client.file.options.OutStreamOptions;
import alluxio.util.ConfigurationUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class OutStreamOptionsDefaultsTest {

    private InstancedConfiguration mConfiguration;
    private ClientContext mClientContext;

    @Before
    public void setUp() {
        // 1. Prepare the test conditions by creating an InstancedConfiguration
        mConfiguration = new InstancedConfiguration(ConfigurationUtils.defaults());
        // 2. Initialize the ClientContext with the prepared configuration
        mClientContext = ClientContext.create(mConfiguration);
    }

    @Test
    public void testOutStreamOptionsDefaults() {
        // 1. Obtain the configuration value dynamically using the Alluxio 2.1.0 API
        int expectedReplicationMin = mConfiguration.getInt(PropertyKey.USER_FILE_REPLICATION_MIN);

        // 2. Create the default OutStreamOptions instance using the ClientContext
        OutStreamOptions outStreamOptions = OutStreamOptions.defaults(mClientContext);

        // 3. Extract the actual replicationMin value from the OutStreamOptions instance
        int actualReplicationMin = outStreamOptions.getReplicationMin();

        // 4. Validate that the actualReplicationMin matches the expectedReplicationMin
        Assert.assertEquals(
            "ReplicationMin value from OutStreamOptions does not match the configured value",
            expectedReplicationMin,
            actualReplicationMin
        );
    }
}