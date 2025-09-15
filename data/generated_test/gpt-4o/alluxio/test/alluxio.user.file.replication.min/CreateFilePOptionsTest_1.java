package alluxio.grpc;

import alluxio.grpc.CreateFilePOptions;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class CreateFilePOptionsTest {

    @Test
    public void test_setReplicationMin_valid_value() {
        // Prepare the test conditions
        CreateFilePOptions.Builder builder = CreateFilePOptions.newBuilder();

        // Test code: Set the replicationMin value to a valid integer, e.g., 5
        int validReplicationMinValue = 5;
        builder.setReplicationMin(validReplicationMinValue);

        // Validate that the internal `replicationMin_` field contains the updated value
        CreateFilePOptions options = builder.build();
        assertEquals(validReplicationMinValue, options.getReplicationMin());
    }
}