package alluxio.network;

import alluxio.conf.AlluxioProperties;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.network.TieredIdentityFactory;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.fail;

public class TieredIdentityFactoryTest {

    @Test
    public void test_fromString_with_unrecognized_tier() {
        // Prepare the test conditions

        // Step 1: Create an instance of AlluxioProperties and InstancedConfiguration
        AlluxioProperties properties = new AlluxioProperties();
        InstancedConfiguration conf = new InstancedConfiguration(properties);

        // Step 2: Use the API to get 'alluxio.locality.order'
        String localityOrder = conf.get(PropertyKey.LOCALITY_ORDER);

        // Step 3: Construct an identity string with an unrecognized tier
        String identityString = "invalid=A,node=B"; // Note: 'invalid' is not part of the configured locality tiers

        // Test Code
        try {
            TieredIdentityFactory.fromString(identityString, conf);
            fail("Expected IOException was not thrown");
        } catch (IOException e) {
            // Assert the exception contains the expected message
            String expectedMessage = "Unrecognized tier: invalid";
            if (!e.getMessage().contains(expectedMessage)) {
                throw new AssertionError(
                    "Expected exception message to contain: " + expectedMessage + 
                    ", but got: " + e.getMessage());
            }
        }

        // Code after testing
        // Cleanup if necessary (not required in this case)
    }
}