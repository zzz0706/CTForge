package alluxio.conf;

import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.conf.AlluxioProperties;
import org.junit.Assert;
import org.junit.Test;

public class ConfigurationValidationTest {

    @Test
    public void validateMasterJournalFlushRetryInterval() {
        // Step 1: Prepare the configuration instance
        AlluxioProperties properties = new AlluxioProperties();
        AlluxioConfiguration configuration = new InstancedConfiguration(properties);

        // Step 2: Read the configuration value using the AlluxioConfiguration API
        String retryIntervalStr = configuration.get(PropertyKey.MASTER_JOURNAL_FLUSH_RETRY_INTERVAL);

        // Step 3: Test the retrieved configuration value
        /*
         * Constraints:
         * 1. The value should be a valid duration string (e.g., "1sec", "500ms", etc.).
         *    - Must be a positive duration.
         *    - Cannot contain an invalid duration format.
         *    - Should not be null or empty.
         */
        Assert.assertNotNull("Configuration value for 'alluxio.master.journal.retry.interval' must not be null", retryIntervalStr);
        Assert.assertFalse("Configuration value for 'alluxio.master.journal.retry.interval' must not be empty", retryIntervalStr.isEmpty());

        // Step 4: Verify that the configuration value satisfies the constraints
        try {
            // Parse the duration to ensure it's a proper format and positive
            long retryIntervalMs = configuration.getMs(PropertyKey.MASTER_JOURNAL_FLUSH_RETRY_INTERVAL);
            Assert.assertTrue("Configuration value for 'alluxio.master.journal.retry.interval' must be a positive duration", retryIntervalMs > 0);
        } catch (IllegalArgumentException e) {
            Assert.fail("Configuration value for 'alluxio.master.journal.retry.interval' is not a valid duration string: " + retryIntervalStr);
        }
    }
}