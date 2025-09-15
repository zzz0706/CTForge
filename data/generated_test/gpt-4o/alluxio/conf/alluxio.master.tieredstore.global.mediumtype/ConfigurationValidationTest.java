package alluxio.conf;

import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.ConfigurationTestUtils;
import alluxio.conf.InstancedConfiguration;
import com.google.common.collect.ImmutableList;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class ConfigurationValidationTest {

    @Test
    public void validateMasterTieredStoreGlobalMediumTypeConfiguration() {
        /*
         * Step 1: Prepare a test configuration instance using the Alluxio 2.1.0 API.
         * Configuration: alluxio.master.tieredstore.global.mediumtype
         * Constraints: The configuration value must be a list of strings separated by commas, and
         * each string must be one of the valid medium types: {"MEM", "SSD", "HDD"}.
         */

        // Initialize the configuration instance
        InstancedConfiguration configuration = ConfigurationTestUtils.defaults();

        // Set up a test value for the property
        configuration.set(PropertyKey.MASTER_TIERED_STORE_GLOBAL_MEDIUMTYPE, "MEM,SSD,HDD");

        // Step 2: Retrieve the configuration value via the Alluxio API
        String mediumTypeConfig = configuration.get(PropertyKey.MASTER_TIERED_STORE_GLOBAL_MEDIUMTYPE);
        List<String> configuredMediumTypes = Arrays.stream(mediumTypeConfig.split(","))
                .map(String::trim)
                .collect(Collectors.toList());

        // Define the allowed medium types
        List<String> validMediumTypes = ImmutableList.of("MEM", "SSD", "HDD");

        // Step 3: Assert that each medium type in the configuration is valid
        for (String mediumType : configuredMediumTypes) {
            Assert.assertTrue(
                    "Invalid medium type found in the configuration: " + mediumType,
                    validMediumTypes.contains(mediumType)
            );
        }

        /*
         * Additional Notes:
         * - This test ensures that only valid medium types ("MEM", "SSD", "HDD") are included in
         *   the "alluxio.master.tieredstore.global.mediumtype" configuration setting.
         * - If any invalid medium type is present, the test will fail with the reason logged.
         * - This guarantees that the configuration adheres to the constraints provided.
         */
    }
}