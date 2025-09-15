package alluxio.client.file.options;

import alluxio.ClientContext;
import alluxio.client.file.options.OutStreamOptions;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.AlluxioProperties;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.grpc.CreateFilePOptions;
import alluxio.util.FileSystemOptions;
import org.junit.Assert;
import org.junit.Test;

public class OutStreamOptionsConfigurationTest {

    @Test
    //test code
    // 1. You need to use the Alluxio 2.1.0 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test code.
    // 4. Code after testing.
    public void test_defaultsReplicationMin_in_OutStreamOptions() {
        // Prepare the Alluxio properties and set USER_FILE_REPLICATION_MIN configuration value
        AlluxioProperties alluxioProperties = new AlluxioProperties();
        final int SAMPLE_REPLICATION_MIN = 2;
        alluxioProperties.set(PropertyKey.USER_FILE_REPLICATION_MIN, String.valueOf(SAMPLE_REPLICATION_MIN));

        // Create a configuration and a ClientContext
        InstancedConfiguration instanceConf = new InstancedConfiguration(alluxioProperties);
        ClientContext context = ClientContext.create(instanceConf);

        // Configure the OutStreamOptions defaults and verify the replicationMin is correctly propagated
        OutStreamOptions options = OutStreamOptions.defaults(context);
        Assert.assertEquals(SAMPLE_REPLICATION_MIN, options.getReplicationMin());
    }

    @Test
    //test code
    // 1. You need to use the Alluxio 2.1.0 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test code.
    // 4. Code after testing.
    public void test_createFileDefaults_PropagationOfReplicationMin() {
        // Prepare the Alluxio properties and set USER_FILE_REPLICATION_MIN
        AlluxioProperties alluxioProperties = new AlluxioProperties();
        final int SAMPLE_REPLICATION_MIN = 3;
        alluxioProperties.set(PropertyKey.USER_FILE_REPLICATION_MIN, String.valueOf(SAMPLE_REPLICATION_MIN));

        // Create an AlluxioConfiguration instance
        InstancedConfiguration instanceConf = new InstancedConfiguration(alluxioProperties);

        // Use the createFileDefaults method to generate file options
        CreateFilePOptions createFileOptions = FileSystemOptions.createFileDefaults(instanceConf);
        Assert.assertEquals(SAMPLE_REPLICATION_MIN, createFileOptions.getReplicationMin());
    }

    @Test
    //test code
    // 1. You need to use the Alluxio 2.1.0 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test code.
    // 4. Code after testing.
    public void test_getDefaultReplication_FromCustomFileSystem() {
        // Prepare the Alluxio properties and set USER_FILE_REPLICATION_MIN
        AlluxioProperties alluxioProperties = new AlluxioProperties();
        final int SAMPLE_REPLICATION_MIN = 4;
        alluxioProperties.set(PropertyKey.USER_FILE_REPLICATION_MIN, String.valueOf(SAMPLE_REPLICATION_MIN));

        // Create an AlluxioConfiguration instance
        InstancedConfiguration instanceConf = new InstancedConfiguration(alluxioProperties);

        // Verify the logic of a custom file system with the USER_FILE_REPLICATION_MIN.
        class CustomFileSystem {
            public short getDefaultReplication() {
                return (short) Math.max(1, instanceConf.getInt(PropertyKey.USER_FILE_REPLICATION_MIN));
            }
        }

        CustomFileSystem customFileSystem = new CustomFileSystem();
        Assert.assertEquals(SAMPLE_REPLICATION_MIN, customFileSystem.getDefaultReplication());
    }
}