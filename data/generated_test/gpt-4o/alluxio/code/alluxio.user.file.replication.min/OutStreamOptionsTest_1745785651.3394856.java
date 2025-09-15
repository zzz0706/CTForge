package alluxio.client.file.options;

import alluxio.ClientContext;
import alluxio.client.file.options.OutStreamOptions;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.AlluxioProperties;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import org.junit.Assert;
import org.junit.Test;

public class OutStreamOptionsTest {

    @Test
    public void test_defaultsReplicationMin_in_OutStreamOptions() {
        // Prepare the test conditions
        // Use AlluxioProperties to create the InstancedConfiguration
        AlluxioProperties alluxioProperties = new AlluxioProperties();
        InstancedConfiguration instanceConf = new InstancedConfiguration(alluxioProperties);
        
        // Create a valid ClientContext
        ClientContext context = ClientContext.create(instanceConf);
        AlluxioConfiguration alluxioConf = instanceConf;

        // Retrieve the `ReplicationMin` configuration value using the API
        int replicationMin = alluxioConf.getInt(PropertyKey.USER_FILE_REPLICATION_MIN);

        // Test code
        // Invoke the `OutStreamOptions.defaults()` method with the `ClientContext`
        OutStreamOptions options = OutStreamOptions.defaults(context);

        // Verify that parsed configuration values propagate and are stored in the instance variable
        Assert.assertEquals(replicationMin, options.getReplicationMin());
    }
}