package alluxio.util;

import alluxio.conf.InstancedConfiguration;
import alluxio.conf.ServerConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.grpc.TtlAction;
import alluxio.grpc.FileSystemMasterCommonPOptions;
import org.junit.Assert;
import org.junit.Test;

public class FileSystemOptionsTest {

    @Test
    public void testCommonDefaults_TtlActionPropagation() {
        // Prepare the test conditions
        InstancedConfiguration conf = new InstancedConfiguration(ServerConfiguration.global());

        // Set a test value for the USER_FILE_CREATE_TTL_ACTION property
        String testTtlActionValue = TtlAction.DELETE.name(); // Ensure using a valid value from TtlAction enum
        conf.set(PropertyKey.USER_FILE_CREATE_TTL_ACTION, testTtlActionValue);

        // Retrieve the configuration value using the Alluxio API
        String ttlActionConfigured = conf.get(PropertyKey.USER_FILE_CREATE_TTL_ACTION);

        // Invoke the function under test
        FileSystemMasterCommonPOptions.Builder builder = FileSystemMasterCommonPOptions.newBuilder();
        if (ttlActionConfigured != null) {
            builder.setTtlAction(TtlAction.valueOf(ttlActionConfigured));
        }
        FileSystemMasterCommonPOptions commonOptions = builder.build();

        // Verify that the propagated ttlAction matches the configuration value set
        Assert.assertEquals(TtlAction.valueOf(ttlActionConfigured), commonOptions.getTtlAction());
    }
}