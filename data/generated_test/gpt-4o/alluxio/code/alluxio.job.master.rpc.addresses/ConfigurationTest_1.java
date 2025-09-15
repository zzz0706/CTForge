package alluxio.client;

import alluxio.conf.AlluxioProperties;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import org.junit.Test;
import static org.junit.Assert.assertEquals;

public class ConfigurationTest {
    //test code
    // 1. You need to use the Alluxio 2.1.0 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test code.
    // 4. Code after testing.

    @Test
    public void testDefaultConfigurationValue() {
        // Prepare the test environment
        AlluxioProperties properties = new AlluxioProperties();
        InstancedConfiguration configuration = new InstancedConfiguration(properties);

        // Use Alluxio API to obtain configuration values
        String defaultValue = configuration.get(PropertyKey.USER_FILE_PASSIVE_CACHE_ENABLED);

        // Perform test assertions
        assertEquals("true", defaultValue); // Assuming the default value from the documentation or source is "true".
    }
}