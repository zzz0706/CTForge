package org.apache.hadoop.security;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.Groups;
import org.apache.hadoop.util.Timer;
import org.junit.Test;

import static org.junit.Assert.assertNull;

public class TestGroupsStaticMapping {
    // get configuration value using API
    // Prepare the input conditions for unit testing.
    @Test
    public void testGroupsConstructorWithEmptyStaticMapping() {
        // Step 1: Create a configuration object.
        Configuration conf = new Configuration();

        // Step 2: Retrieve the `hadoop.user.group.static.mapping.overrides` property using the API.
        // Default value for an unset configuration is empty.
        String staticMapping = conf.get(CommonConfigurationKeys.HADOOP_USER_GROUP_STATIC_OVERRIDES);

        // Step 3: Instantiate the `Groups` class using configuration and a mock Timer.
        Timer mockTimer = new Timer();
        Groups groups = new Groups(conf, mockTimer);

        // Step 4: Verify that the internal `staticMapRef` is null, since no static mappings are provided.
        assertNull("The staticMapRef should be null when the static mapping is empty.", groups.getStaticMap());
    }
}