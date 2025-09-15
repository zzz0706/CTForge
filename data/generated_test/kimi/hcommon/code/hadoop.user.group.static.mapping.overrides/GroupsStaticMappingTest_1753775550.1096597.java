package org.apache.hadoop.security;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ Groups.class, ShellBasedUnixGroupsMapping.class })
public class GroupsStaticMappingTest {

    @Test
    public void verifyGroupsCallUsesStaticMappingInsteadOfShellLookup() throws Exception {
        // 1. Use the HDFS 2.8.5 API correctly to obtain configuration values
        Configuration conf = new Configuration();
        // Ensure we start with the default (empty) value
        conf.set(CommonConfigurationKeys.HADOOP_USER_GROUP_STATIC_OVERRIDES, "");

        // 2. Prepare the test conditions
        String staticMapping = conf.get(
                CommonConfigurationKeys.HADOOP_USER_GROUP_STATIC_OVERRIDES,
                CommonConfigurationKeys.HADOOP_USER_GROUP_STATIC_OVERRIDES_DEFAULT);
        assertEquals("", staticMapping); // Ensure we start clean

        // Set the static mapping via the API
        conf.set(CommonConfigurationKeys.HADOOP_USER_GROUP_STATIC_OVERRIDES, "mallory=shadow");

        // Mock the shell-based mapping to ensure it's never called
        ShellBasedUnixGroupsMapping shellSpy = spy(new ShellBasedUnixGroupsMapping());
        PowerMockito.whenNew(ShellBasedUnixGroupsMapping.class).withNoArguments().thenReturn(shellSpy);

        // 3. Test code
        Groups groups = new Groups(conf);
        List<String> actualGroups = groups.getGroups("mallory");

        // 4. Code after testing
        assertEquals(1, actualGroups.size());
        assertEquals("shadow", actualGroups.get(0));
        verify(shellSpy, never()).getGroups("mallory");
    }
}