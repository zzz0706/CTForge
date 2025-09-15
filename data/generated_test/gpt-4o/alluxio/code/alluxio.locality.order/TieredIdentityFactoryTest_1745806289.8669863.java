package alluxio.network;

import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.AlluxioProperties;
import alluxio.conf.PropertyKey;
import alluxio.network.TieredIdentityFactory;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

public class TieredIdentityFactoryTest {

    // Test case: test_fromString_with_conflicting_tier_definitions
    // Objective: Verify that fromString throws an exception when the identity string contains repeated definitions for the same tier.
    @Test
    public void testFromStringWithConflictingTierDefinitions() {
        // 1. Prepare the test conditions.
        // Create an AlluxioProperties instance for configuration.
        AlluxioProperties properties = new AlluxioProperties();
        // Use the AlluxioProperties to construct an InstancedConfiguration instance.
        AlluxioConfiguration conf = new InstancedConfiguration(properties);

        // Ensure 'alluxio.locality.order' is set as per the configuration.
        properties.set(PropertyKey.LOCALITY_ORDER, "node,rack,dc");

        // Obtain the list of tiers to confirm configuration is correct.
        String tierOrder = conf.get(PropertyKey.LOCALITY_ORDER);
        Assert.assertEquals("node,rack,dc", tierOrder);

        // Construct an identity string with conflicting tier definitions.
        String conflictingIdentityString = "node=A,node=B";

        // 2. Test code.
        try {
            // Invoke the fromString method with the identity string and configuration instance.
            TieredIdentityFactory.fromString(conflictingIdentityString, conf);

            // If no exception is thrown, the test fails.
            Assert.fail("Expected IOException due to conflicting tier definitions, but none was thrown.");
        } catch (IOException e) {
            // 3. Verify expected results.
            String expectedMessage = "Encountered repeated tier definition"; // Part of the expected exception message.
            Assert.assertTrue("Exception message does not match expected pattern.",
                e.getMessage().contains(expectedMessage));
        }
    }
}