package alluxio.client.file.options;

import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.AlluxioProperties;
import alluxio.grpc.CreateFilePOptions;
import alluxio.util.FileSystemOptions;
import org.junit.Assert;
import org.junit.Test;

public class FileSystemOptionsTest {
    // test code
    // 1. You need to use the Alluxio 2.1.0 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test code.
    // 4. Code after testing.

    @Test
    public void testCreateFileDefaults() {
        // Prepare test conditions: Initialize AlluxioProperties and InstancedConfiguration
        AlluxioProperties properties = new AlluxioProperties();
        AlluxioConfiguration conf = new InstancedConfiguration(properties);

        // Obtain the configuration value using the API
        int expectedReplicationMin = conf.getInt(alluxio.conf.PropertyKey.USER_FILE_REPLICATION_MIN);

        // Test code: Generate CreateFilePOptions using createFileDefaults
        CreateFilePOptions options = FileSystemOptions.createFileDefaults(conf);

        // Extracting the replicationMin property and validating
        int actualReplicationMin = options.getReplicationMin();

        // Code after testing
        Assert.assertEquals("ReplicationMin should match the configuration value", expectedReplicationMin, actualReplicationMin);
    }
}