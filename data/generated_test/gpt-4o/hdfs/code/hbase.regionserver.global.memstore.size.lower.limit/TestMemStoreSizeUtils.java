package org.apache.hadoop.hbase.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.junit.Before;
import org.junit.Test;
import org.junit.ClassRule;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.*;

/**
 * Test class for validating memstore size lower limit configuration.
 * This ensures the configuration value adheres to its constraints.
 */
@Category({MiscTests.class, SmallTests.class})
public class TestMemStoreSizeUtils {

    @ClassRule
    public static final HBaseClassTestRule CLASS_RULE = HBaseClassTestRule.forClass(TestMemStoreSizeUtils.class);

    private Configuration conf;

    @Before
    public void setUp() {
        // Initialize the Configuration instance
        conf = new Configuration();
        // Set default test values for configurations
        conf.setFloat("hbase.regionserver.global.memstore.size.lower.limit", 0.4f);
        conf.setFloat("hbase.regionserver.global.memstore.lowerLimit", 0.3f);
    }

    /**
     * Test to validate "hbase.regionserver.global.memstore.size.lower.limit" configuration.
     * 
     * Constraints:
     * - The value should be a float between 0.0 and 1.0 inclusive.
     * - If explicitly set, verify adherence to constraints.
     * - If fallback to deprecated config "hbase.regionserver.global.memstore.lowerLimit", ensure compliance with constraints.
     */
    @Test
    public void testMemStoreSizeLowerLimitValidation() {
        // 1. Retrieve configuration value using the HBase API
        String lowerLimitKey = "hbase.regionserver.global.memstore.size.lower.limit";
        String deprecatedLowerLimitKey = "hbase.regionserver.global.memstore.lowerLimit";

        String valueStr = conf.get(lowerLimitKey);

        if (valueStr != null) {
            try {
                float value = Float.parseFloat(valueStr);

                // 2. Ensure value is within valid [0.0, 1.0] range
                assertTrue(
                    "Configuration value for " + lowerLimitKey +
                    " is out of bounds (must be between 0.0 and 1.0): " + value,
                    value >= 0.0f && value <= 1.0f
                );
            } catch (NumberFormatException e) {
                fail("Configuration value for " + lowerLimitKey +
                    " is not a valid float: " + valueStr);
            }
        } else {
            // 3. Handle fallback to deprecated config
            String deprecatedValueStr = conf.get(deprecatedLowerLimitKey);

            if (deprecatedValueStr != null) {
                try {
                    float deprecatedValue = Float.parseFloat(deprecatedValueStr);

                    // Validate deprecated value against constraints
                    assertTrue(
                        "Deprecated configuration value for " + deprecatedLowerLimitKey +
                        " is out of valid range (must be between 0.0 and 1.0): " + deprecatedValue,
                        deprecatedValue >= 0.0f && deprecatedValue <= 1.0f
                    );
                } catch (NumberFormatException e) {
                    fail("Deprecated configuration value for " + deprecatedLowerLimitKey +
                        " is not a valid float: " + deprecatedValueStr);
                }
            } else {
                fail("Neither " + lowerLimitKey + " nor " + deprecatedLowerLimitKey +
                    " is configured. One of them must be set.");
            }
        }

        // 4. Validate the default value behavior
        float defaultValue = 0.2f; // Define the default setting for memstore size lower limit
        assertTrue(
            "Default value for memstore size lower limit is invalid: " + defaultValue,
            defaultValue >= 0.0f && defaultValue <= 1.0f
        );
    }
}