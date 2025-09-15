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
        // 3. Get the current configuration value for alluxio.user.file.replication.min
        int replicationMinValue = mConfiguration.getInt(PropertyKey.USER_FILE_REPLICATION_MIN);

        // 4. Create an OutStreamOptions instance with the defaults
        OutStreamOptions outStreamOptions = OutStreamOptions.defaults(mClientContext);

        // 5. Extract the replicationMin value from the returned OutStreamOptions instance
        int actualReplicationMin = outStreamOptions.getReplicationMin();

        // 6. Assert that the replicationMin value matches the value of USER_FILE_REPLICATION_MIN
        Assert.assertEquals(
            "ReplicationMin value does not match USER_FILE_REPLICATION_MIN configuration",
            replicationMinValue,
            actualReplicationMin
        );
    }
}