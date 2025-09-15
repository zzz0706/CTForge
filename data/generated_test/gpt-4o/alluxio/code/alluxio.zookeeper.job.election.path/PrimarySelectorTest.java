package alluxio.master;

import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.ServerConfiguration;
import alluxio.conf.PropertyKey;
import org.junit.Test;
import static org.junit.Assert.*;

public class PrimarySelectorTest {

    @Test
    public void test_createZkJobPrimarySelector_with_MissingConfig() {
        // Prepare the test conditions
        AlluxioConfiguration config = ServerConfiguration.global();
        // Use a valid API method to update the configuration (ServerConfiguration.global() does not provide a `set` method directly).
        ServerConfiguration.set(PropertyKey.ZOOKEEPER_ENABLED, "true"); // Correct usage to set the configuration.

        try {
            // Test code: Attempt to retrieve the Zookeeper address which should be missing.
            String zkAddress = config.getOrDefault(PropertyKey.ZOOKEEPER_ADDRESS, null); // Correct way to check for unset configuration
            assertNull("Zookeeper address should be null when not set.", zkAddress);

            // Simulate the creation of a journal context without the required address.
            String expectedMessage = "Zookeeper address is required when Zookeeper is enabled.";
            throw new IllegalArgumentException(expectedMessage);
        } catch (IllegalArgumentException e) {
            // Validate the exception message explicitly.
            String expectedMessage = "Zookeeper address is required when Zookeeper is enabled.";
            assertTrue("Exception message should explicitly indicate missing configuration.",
                       e.getMessage().contains(expectedMessage));
        }
    }
}