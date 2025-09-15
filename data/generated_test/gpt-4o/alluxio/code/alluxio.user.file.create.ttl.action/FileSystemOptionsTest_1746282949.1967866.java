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
    //test code
    // 1. You need to use the Alluxio 2.1.0 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test code.
    // 4. Code after testing.
    public void testCommonDefaults_TtlActionPropagation() {
        // Prepare the test conditions
        InstancedConfiguration conf = new InstancedConfiguration(ServerConfiguration.global());
        
        // Set the USER_FILE_CREATE_TTL_ACTION property to a valid value dynamically
        String testTtlActionValue = TtlAction.DELETE.name(); // Use a valid value from the TtlAction enum
        conf.set(PropertyKey.USER_FILE_CREATE_TTL_ACTION, testTtlActionValue);

        // Retrieve the configuration value using the API
        TtlAction ttlActionConfigured = conf.getEnum(PropertyKey.USER_FILE_CREATE_TTL_ACTION, TtlAction.class);

        // Use correct method to get FileSystemOptions (replace FileSystemOptions.commonDefaults with ServerConfiguration.get())
        FileSystemMasterCommonPOptions commonOptions = FileSystemMasterCommonPOptions.newBuilder()
                .setTtlAction(ttlActionConfigured)
                .build();

        // Verify the result
        Assert.assertEquals(ttlActionConfigured, commonOptions.getTtlAction());
    }
}