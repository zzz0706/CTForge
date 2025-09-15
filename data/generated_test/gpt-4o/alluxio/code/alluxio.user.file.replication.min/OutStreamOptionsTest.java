package alluxio.client.file.options;

import alluxio.ConfigurationTestUtils;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.grpc.CreateFilePOptions;
import alluxio.util.FileSystemOptions;
import org.junit.Assert;
import org.junit.Test;

public class OutStreamOptionsTest {
    //test code
    @Test
    public void test_createFileDefaults_replicationMin_propagation() {
        // 1. You need to use the Alluxio 2.1.0 API correctly to obtain configuration values, instead of hardcoding the configuration values.

        // Prepare test conditions using a test utility provided by Alluxio
        AlluxioConfiguration alluxioConf = ConfigurationTestUtils.defaults();

        // Use API to retrieve the configuration value for replication minimum
        int replicationMinConfigValue = alluxioConf.getInt(PropertyKey.USER_FILE_REPLICATION_MIN);

        // 2. Prepare the test conditions
        // Invoke createFileDefaults() from FileSystemOptions
        CreateFilePOptions fileOptions = FileSystemOptions.createFileDefaults(alluxioConf);

        // 3. Test code
        // Verify the returned CreateFilePOptions object fetches the correct value of ReplicationMin
        Assert.assertEquals(replicationMinConfigValue, fileOptions.getReplicationMin());

        // Further assertions can be performed as needed
        int rpcReplicationValue = fileOptions.getReplicationMin();
        Assert.assertEquals(replicationMinConfigValue, rpcReplicationValue);

        // 4. Code after testing
        // Cleanup or additional assertions (if necessary) can be added here
    }
}