package alluxio.conf;    

import alluxio.conf.AlluxioProperties;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import org.junit.Assert;
import org.junit.Test;

public class ConfigurationValidationTest {

    /**
     * Test to validate the correctness of the configuration value for
     * `alluxio.master.embedded.journal.storage.level`.
     */
    @Test
    public void validateEmbeddedJournalStorageLevelConfiguration() {
        // Step 1: Prepare the Alluxio properties
        AlluxioProperties alluxioProperties = new AlluxioProperties();

        // Step 2: Create the InstancedConfiguration instance using the properties
        InstancedConfiguration configuration = new InstancedConfiguration(alluxioProperties);

        // Step 3: Retrieve the configuration value
        String storageLevelConfig = configuration.get(PropertyKey.MASTER_EMBEDDED_JOURNAL_STORAGE_LEVEL);

        // Step 4: Validate the configuration value
        try {
            // Validate against expected values for MASTER_EMBEDDED_JOURNAL_STORAGE_LEVEL which are typically disk, memory, mapped, etc.
            Assert.assertTrue("Unexpected storage level",
                    "DISK".equalsIgnoreCase(storageLevelConfig) ||
                    "MAPPED".equalsIgnoreCase(storageLevelConfig) ||
                    "MEMORY".equalsIgnoreCase(storageLevelConfig));
        } catch (Exception e) {
            Assert.fail("Invalid value for alluxio.master.embedded.journal.storage.level: " + storageLevelConfig);
        }
    }
}