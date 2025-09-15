package alluxio.conf;

import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.conf.AlluxioProperties;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class ConfigurationValidationTest {

    @Test
    public void testMasterWhitelistConfiguration() {
        // Step 1: Initialize AlluxioProperties and use it to create an InstancedConfiguration.
        AlluxioProperties alluxioProperties = new AlluxioProperties();
        AlluxioConfiguration configuration = new InstancedConfiguration(alluxioProperties);

        // Step 2: Get the configuration value for 'alluxio.master.whitelist'.
        String whitelistString = configuration.get(PropertyKey.MASTER_WHITELIST);
        List<String> whitelist = whitelistString.isEmpty() ? 
                                  Arrays.asList() :
                                  Arrays.asList(whitelistString.split(","));

        // Step 3: Validate the whitelist configuration against constraints.
        /*
         * Constraint 1: The configuration value can be empty, which means it allows all paths to be cacheable.
         * Constraint 2: If the configuration is not empty, it must be a valid list of path prefixes.
         */

        Assert.assertNotNull("Configuration value for alluxio.master.whitelist should not be null", whitelist);

        for (String prefix : whitelist) {
            // Validate that each entry in the whitelist is a valid path prefix.
            // Here we assume that path prefixes should start with a '/' similar to POSIX file paths.
            Assert.assertTrue("Each entry in alluxio.master.whitelist must start with '/'",
                    prefix.startsWith("/"));

            // Additional check to ensure no invalid characters in the path prefix (custom logic can go here).
            boolean isValidPrefix = prefix.matches("^[a-zA-Z0-9/_-]*$");
            Assert.assertTrue("Each entry in alluxio.master.whitelist must be a valid path prefix", isValidPrefix);
        }
    }
}