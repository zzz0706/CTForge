package org.apache.hadoop.security;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.util.Time;
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
        // 1. Configuration as input
        Configuration conf = new Configuration();
        // 2. Dynamic expected value calculation
        String staticMapping = conf.get(
                CommonConfigurationKeys.HADOOP_USER_GROUP_STATIC_OVERRIDES,
                CommonConfigurationKeys.HADOOP_USER_GROUP_STATIC_OVERRIDES_DEFAULT);
        // Compute expected value from the parsed mapping
        String expectedGroup = "shadow";
        conf.set(CommonConfigurationKeys.HADOOP_USER_GROUP_STATIC_OVERRIDES, "mallory=" + expectedGroup);

        // 3. Mock/Stub external dependencies
        ShellBasedUnixGroupsMapping shellSpy = spy(new ShellBasedUnixGroupsMapping());
        PowerMockito.whenNew(ShellBasedUnixGroupsMapping.class).withNoArguments().thenReturn(shellSpy);

        // 4. Invoke the method under test
        Groups groups = new Groups(conf);
        List<String> actualGroups = groups.getGroups("mallory");

        // 5. Assertions and verification
        assertEquals(1, actualGroups.size());
        assertEquals(expectedGroup, actualGroups.get(0));
        verify(shellSpy, never()).getGroups("mallory");
    }
}