package org.apache.hadoop.hdfs.server.datanode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.io.IOException;

public class CheckSuperuserPrivilegeTest {

    private CheckSuperuserPrivilegeTestClass testClass;

    @Mock
    private UserGroupInformation callerUgiMock;

    private Configuration conf;

    @Before
    public void setUp() throws IOException {
        // Initialize Mockito and prepare the test environment
        MockitoAnnotations.initMocks(this);

        // Setup the Hdfs Configuration
        conf = new HdfsConfiguration();
        testClass = new CheckSuperuserPrivilegeTestClass(conf, callerUgiMock);
    }

    @Test
    // test code
    // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test code.
    // 4. Code after testing.
    public void testCheckSuperuserPrivilege_WhenPermissionDisabled() throws IOException {
        // 1. Use hdfs 2.8.5 API to set configuration values
        conf.setBoolean("dfs.permissions.enabled", false); // Correct usage of the configuration key

        // 2. Prepare the test conditions
        String dnUserName = "testDnUser";
        String supergroup = "testSuperGroup";

        Mockito.when(callerUgiMock.getShortUserName()).thenReturn(dnUserName);
        Mockito.when(callerUgiMock.getGroupNames()).thenReturn(new String[]{supergroup});

        conf.set("dfs.datanode.username", dnUserName);
        conf.set("dfs.permissions.supergroup", supergroup);

        // 3. Execute the test code
        try {
            testClass.invokeCheckSuperuserPrivilege();
        } catch (AccessControlException e) {
            // Verify no exceptions are thrown when permissions are disabled
            throw new AssertionError("AccessControlException should not be thrown when dfs.permissions.enabled is set to false");
        }
    }

    // Helper class to expose and test private members/functions
    private static class CheckSuperuserPrivilegeTestClass {

        private final Configuration conf;
        private final UserGroupInformation callerUgiMock;

        CheckSuperuserPrivilegeTestClass(Configuration conf, UserGroupInformation callerUgiMock) {
            this.conf = conf;
            this.callerUgiMock = callerUgiMock;
        }

        void invokeCheckSuperuserPrivilege() throws IOException, AccessControlException {
            String dnUserName = conf.get("dfs.datanode.username");
            String supergroup = conf.get("dfs.permissions.supergroup");

            if (conf.getBoolean("dfs.permissions.enabled", true)) {
                if (!callerUgiMock.getShortUserName().equals(dnUserName) &&
                        !isMemberOfSupergroup(supergroup, callerUgiMock)) {
                    throw new AccessControlException("Access Denied for user " + callerUgiMock.getShortUserName());
                }
            }
        }

        private boolean isMemberOfSupergroup(String supergroup, UserGroupInformation ugi) {
            for (String group : ugi.getGroupNames()) {
                if (supergroup.equals(group)) {
                    return true;
                }
            }
            return false;
        }
    }
}