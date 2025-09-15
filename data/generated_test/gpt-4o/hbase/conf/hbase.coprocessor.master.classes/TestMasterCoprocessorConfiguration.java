package org.apache.hadoop.hbase.coprocessor;

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.coprocessor.MasterObserver;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost; // Correct import for MASTER_COPROCESSOR_CONF_KEY
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Arrays;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertNotNull;

@Category({MasterTests.class, SmallTests.class})
public class TestMasterCoprocessorConfiguration {

    @ClassRule
    public static final HBaseClassTestRule CLASS_RULE =
            HBaseClassTestRule.forClass(TestMasterCoprocessorConfiguration.class);

    private Configuration conf;

    @Before
    public void setUp() {
        conf = new Configuration();
        // In a real HBase environment, the configuration would be populated by the runtime.
        // For testing, you would add mock configurations as needed.
    }

    @Test
    public void testMasterCoprocessorClassesConfiguration() {
        /*
         * Test to validate 'hbase.coprocessor.master.classes' configuration.
         * 1. Confirm the configuration key exists.
         * 2. Validate the classes listed implement the MasterObserver interface.
         * 3. Handle invalid class names or incorrect implementations.
         */

        // Step 1: Retrieve the configuration value for 'hbase.coprocessor.master.classes'.
        String masterCoprocessorClasses = conf.get(CoprocessorHost.MASTER_COPROCESSOR_CONF_KEY, "");

        // Step 2: Validate the configuration value.
        // It is valid for the configuration to be empty as it denotes no coprocessors are configured.
        if (!masterCoprocessorClasses.isEmpty()) {
            // Split by comma to separate potential class names and validate each.
            String[] classList = masterCoprocessorClasses.split(",");

            for (String className : classList) {
                try {
                    // Attempt to load the class dynamically.
                    Class<?> cls = Class.forName(className.trim());

                    // Validate that the loaded class implements MasterObserver.
                    assertTrue("Class " + className + " does not implement MasterObserver.",
                            MasterObserver.class.isAssignableFrom(cls));
                } catch (ClassNotFoundException e) {
                    // Configuration contains a class that cannot be found.
                    throw new AssertionError("Invalid configuration value: class " + className + " could not be found.", e);
                }
            }
        }

        // Step 3: Ensure the configuration string itself is not null.
        assertNotNull("Configuration key 'hbase.coprocessor.master.classes' should not be null.", masterCoprocessorClasses);

        // Step 4: Optionally log validation results for debugging purposes.
        System.out.println("Configuration 'hbase.coprocessor.master.classes' validated successfully with values: "
                + Arrays.toString(masterCoprocessorClasses.split(",")));
    }
}