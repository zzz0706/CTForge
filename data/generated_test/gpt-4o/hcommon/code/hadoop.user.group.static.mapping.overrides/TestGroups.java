package org.apache.hadoop.security;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.Test;
import static org.junit.Assert.*;

public class TestGroups {

    // get configuration value using API
    // Prepare the input conditions for unit testing.
    @Test
    public void testGroupsConstructorWithMalformedStaticMapping() {
        // Prepare configuration with malformed static mapping
        Configuration conf = new Configuration();
        conf.set(CommonConfigurationKeys.HADOOP_USER_GROUP_STATIC_OVERRIDES, "user1==group1;user2;;user3");

        // Verify that the Groups constructor throws the specific exception
        Exception exception = assertThrows(HadoopIllegalArgumentException.class, () -> {
            new Groups(conf, GenericTestUtils.getSourceForTest());
        });

        // Check the exception message to ensure correctness
        String expectedMessage = "Configuration " + CommonConfigurationKeys.HADOOP_USER_GROUP_STATIC_OVERRIDES + " is invalid";
        assertTrue(exception.getMessage().contains(expectedMessage));
    }
}