package org.apache.hadoop.test;

import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.Test;

public class TestHadoopRpcProtectionConfig {

    @Test
    public void testHadoopRpcProtectionConfig() {
        // Initialize Hadoop Configuration object
        Configuration configuration = new Configuration();

        // Retrieve the configuration value for "hadoop.rpc.protection"
        String[] rpcProtectionValues = configuration.getTrimmedStrings(
                "hadoop.rpc.protection",
                "authentication"  // Correcting from QualityOfProtection.AUTHENTICATION to the string representation "authentication"
        );

        // Valid values for "hadoop.rpc.protection"
        String[] validValues = {
                "authentication",  // Correcting from QualityOfProtection.AUTHENTICATION.toString()
                "integrity",       // Correcting from QualityOfProtection.INTEGRITY.toString()
                "privacy"          // Correcting from QualityOfProtection.PRIVACY.toString()
        };

        // Step 1: Ensure the retrieved values are not null/empty
        Assert.assertNotNull("The 'hadoop.rpc.protection' configuration should not be null", rpcProtectionValues);
        Assert.assertTrue("The 'hadoop.rpc.protection' configuration should contain at least one value",
                rpcProtectionValues.length > 0);

        // Step 2: Validate each value in the retrieved configuration
        for (String value : rpcProtectionValues) {
            boolean isValid = false;

            for (String validValue : validValues) {
                if (validValue.equalsIgnoreCase(value)) { // Simplifying comparison logic
                    isValid = true;
                    break;
                }
            }

            Assert.assertTrue(
                    "Invalid value '" + value + "' found in the 'hadoop.rpc.protection' configuration. Valid values are: " +
                            String.join(", ", validValues), isValid);
        }

        // Step 3: Test dependency with "hadoop.security.saslproperties.resolver.class"
        String saslPropertiesResolverClass = configuration.get("hadoop.security.saslproperties.resolver.class");

        if (saslPropertiesResolverClass != null) {
            try {
                // Ensure the class can be loaded
                Class<?> clazz = Class.forName(saslPropertiesResolverClass);
                Assert.assertTrue(
                        "The class specified by 'hadoop.security.saslproperties.resolver.class' must be a subclass of org.apache.hadoop.security.SaslPropertiesResolver",
                        org.apache.hadoop.security.SaslPropertiesResolver.class.isAssignableFrom(clazz));
            } catch (ClassNotFoundException e) {
                Assert.fail("The class specified by 'hadoop.security.saslproperties.resolver.class' could not be found: " + e.getMessage());
            }
        }
    }
}