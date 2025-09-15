package alluxio.client.file.options;

import org.junit.Test;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.grpc.CreateFilePOptions;

import static org.junit.Assert.assertEquals;

public class CreateFilePOptionsTest {
    // Test method for CreateFilePOptions.setReplicationMin
    @Test
    public void testSetReplicationMin() {
        // 1. You need to use the Alluxio 2.1.0 API correctly to obtain configuration values, instead of hardcoding the configuration values.
        InstancedConfiguration conf = InstancedConfiguration.defaults();
        int expectedReplicationMin = conf.getInt(PropertyKey.USER_FILE_REPLICATION_MIN);

        // 2. Prepare the test conditions by instantiating a CreateFilePOptions.Builder object.
        CreateFilePOptions.Builder builder = CreateFilePOptions.newBuilder();

        // 3. Use the setReplicationMin method to set the replicationMin value (obtained via API).
        builder.setReplicationMin(expectedReplicationMin);

        // 4. Verify the replicationMin field in the Builder reflects the expected value.
        CreateFilePOptions options = builder.build();
        assertEquals(expectedReplicationMin, options.getReplicationMin());
    }
}