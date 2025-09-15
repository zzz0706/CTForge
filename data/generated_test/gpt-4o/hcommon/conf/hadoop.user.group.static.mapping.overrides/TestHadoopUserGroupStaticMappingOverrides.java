package org.apache.hadoop.security;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.util.StringUtils;
import org.junit.Test;

import java.util.*;

import static org.junit.Assert.*;

public class TestHadoopUserGroupStaticMappingOverrides {

    /**
     * Test to verify that the configuration 'hadoop.user.group.static.mapping.overrides'
     * adheres to constraints.
     */
    @Test
    public void testStaticMappingConfiguration() {
        Configuration conf = new Configuration();
        String staticMapping = conf.get(
                CommonConfigurationKeys.HADOOP_USER_GROUP_STATIC_OVERRIDES,
                CommonConfigurationKeys.HADOOP_USER_GROUP_STATIC_OVERRIDES_DEFAULT);

        // Step 1: Parse the configuration value into a collection of mappings
        Collection<String> mappings = StringUtils.getStringCollection(staticMapping, ";");
        Map<String, List<String>> parsedMappings = new HashMap<>();

        for (String mapping : mappings) {
            Collection<String> userToGroups = StringUtils.getStringCollection(mapping, "=");
            
            // Step 2: Validate number of tokens (either 1 or 2 separated by "=")
            if (userToGroups.size() < 1 || userToGroups.size() > 2) {
                fail("Invalid configuration for 'hadoop.user.group.static.mapping.overrides': " +
                        "Mapping '" + mapping + "' does not have 1 or 2 parts separated by '='.");
            }

            // Convert userToGroups collection to array for further processing
            String[] userToGroupsArray = userToGroups.toArray(new String[userToGroups.size()]);
            String user = userToGroupsArray[0]; // Extract user
            List<String> groups = Collections.emptyList();
            
            // If there are groups defined for the user, parse them into a list
            if (userToGroupsArray.length == 2) {
                groups = (List<String>) StringUtils.getStringCollection(userToGroupsArray[1]);
            }

            // Check case where a user is defined but groups are empty explicitly
            if (userToGroupsArray.length == 2 && groups.isEmpty()) {
                // Validate that the user can be without any groups
                assertTrue("User '" + user + "' has no groups but wrong format detected.",
                        userToGroupsArray[1].trim().isEmpty());
            }

            // Add the parsed user and groups to the mapping list
            parsedMappings.put(user, groups);
        }

        // Final assertions for validity
        assertNotNull("The parsed mapping should not be null.", parsedMappings);

        // If there is a default value present (e.g., "dr.who=;"), confirm that it adheres to the constraint
        assertTrue("Default value for 'dr.who' should be valid (without groups).",
                parsedMappings.containsKey("dr.who") && parsedMappings.get("dr.who").isEmpty());
    }
}