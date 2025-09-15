package alluxio.client.file.options;

import org.junit.Test;
import org.junit.Assert;
import alluxio.conf.AlluxioProperties;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.grpc.CreateFilePOptions;

public class CreateFilePOptionsTest {
    // test code
    // 1. You need to use the Alluxio 2.1.0 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test code.
    // 4. Code after testing.

    @Test
    public void test_setReplicationMin_in_CreateFilePOptionsBuilder() {
        // Step 1: Initialize the configuration using AlluxioProperties and InstancedConfiguration API
        AlluxioProperties props = new AlluxioProperties();
        InstancedConfiguration alluxioConf = new InstancedConfiguration(props);

        // Step 2: Prepare the builder instance of `CreateFilePOptions`
        CreateFilePOptions.Builder fileOptionsBuilder = CreateFilePOptions.newBuilder();

        // Step 3: Set ReplicationMin using the value fetched via AlluxioConfiguration API
        int replicationMin = alluxioConf.getInt(PropertyKey.USER_FILE_REPLICATION_MIN);
        fileOptionsBuilder.setReplicationMin(replicationMin);

        // Step 4: Verify the builder correctly sets and retains the `ReplicationMin` value
        CreateFilePOptions fileOptions = fileOptionsBuilder.build();
        Assert.assertEquals(replicationMin, fileOptions.getReplicationMin());
    }
}