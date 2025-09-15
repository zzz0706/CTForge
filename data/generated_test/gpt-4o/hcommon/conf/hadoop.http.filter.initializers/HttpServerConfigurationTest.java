package org.apache.hadoop.http;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.http.FilterInitializer;
import org.apache.hadoop.util.ReflectionUtils;
import org.junit.Test;

import static org.junit.Assert.*;

public class HttpServerConfigurationTest {

    /**
     * Test to validate hadoop.http.filter.initializers configuration values.
     */
    @Test
    public void testFilterInitializersConfiguration() {
        // Load configuration
        Configuration conf = new Configuration();

        // Step 1: Obtain the hadoop.http.filter.initializers property value
        Class<?>[] classes = conf.getClasses("hadoop.http.filter.initializers");

        // Step 2: Validate property value (should be an array of classes that extend FilterInitializer)
        if (classes != null) {
            for (Class<?> clazz : classes) {
                // Ensure it is not null
                assertNotNull("FilterInitializer class is null", clazz);

                // Ensure the class extends FilterInitializer
                assertTrue(
                    "Class " + clazz.getName() + " does not implement FilterInitializer",
                    FilterInitializer.class.isAssignableFrom(clazz)
                );

                // Ensure the class can be instantiated (e.g., has a valid constructor)
                try {
                    FilterInitializer instance = (FilterInitializer) ReflectionUtils.newInstance(clazz, conf);
                    assertNotNull("Unable to instantiate FilterInitializer from class " + clazz.getName(), instance);
                } catch (Exception e) {
                    fail("Instantiation of FilterInitializer class " + clazz.getName() + " caused an exception: " + e.getMessage());
                }
            }
        } else {
            // Ensure the property is allowed to be null
            // By configuration default, an empty or non-existent value should be allowed.
            assertNull("Configuration returned non-null for empty hadoop.http.filter.initializers", classes);
        }
    }
}