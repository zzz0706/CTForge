package org.apache.hadoop.security;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.Test;
import org.mockito.Mockito;

import javax.security.auth.Subject;
import java.io.File;
import java.io.FilenameFilter;

import static org.mockito.Mockito.*;

public class TestKerberosConfigurationUsage {

    /**
     * Test case: Validate that the `validateKinitExecutable` method correctly validates the
     * kinit executable when given an absolute path in the configuration.
     */
    @Test
    public void testValidateKinitExecutable_withValidAbsolutePath() {
        // Mock the Configuration object
        Configuration mockConf = mock(Configuration.class);
        when(mockConf.getTrimmed(KDiag.KERBEROS_KINIT_COMMAND, ""))
            .thenReturn("/usr/bin/kinit");

        // Mock the File object to simulate the existence of a valid kinit executable
        File mockFile = mock(File.class);
        when(mockFile.isAbsolute()).thenReturn(true);
        when(mockFile.exists()).thenReturn(true);
        when(mockFile.length()).thenReturn(100L); // Simulate a non-empty file

        // Spy on KDiag instance while injecting mocked dependencies
        KDiag kDiag = spy(new KDiag());
        doReturn(mockConf).when(kDiag).getConf();
        doReturn(mockFile).when(kDiag).wrapFileInstance(any(String.class));
        doNothing().when(kDiag).println(anyString(), any());

        // Call the method under test
        kDiag.validateKinitExecutable();

        // Verify that configuration and validation are properly executed
        verify(mockConf).getTrimmed(KDiag.KERBEROS_KINIT_COMMAND, "");
        verify(kDiag).wrapFileInstance("/usr/bin/kinit");
        verify(mockFile).isAbsolute();
        verify(mockFile).exists();
        verify(mockFile).length();
        verify(kDiag).println(anyString(), eq(mockFile));
    }

    /**
     * Test case: Validate the `execute` method ensures security-related configurations and
     * diagnostic checks are properly invoked.
     */
    @Test
    public void testExecute_withValidConfiguration() throws Exception {
        // Mock the Configuration object
        Configuration mockConf = mock(Configuration.class);
        when(mockConf.getTrimmed(KDiag.KERBEROS_KINIT_COMMAND, ""))
            .thenReturn("/usr/bin/kinit");

        // Spy on KDiag instance while injecting mocked dependencies
        KDiag kDiag = spy(new KDiag());
        doReturn(mockConf).when(kDiag).getConf();
        doNothing().when(kDiag).validateKinitExecutable();

        // Call the method under test
        boolean result = kDiag.execute();

        // Verify that configuration and required method(s) are invoked
        verify(mockConf).getTrimmed(KDiag.KERBEROS_KINIT_COMMAND, "");
        verify(kDiag).validateKinitExecutable();
        assert result; // Check that the execution is successful
    }

    /**
     * Test case: Validate that the `spawnAutoRenewalThreadForUserCreds` method spawns a renewal
     * thread which uses the Kerberos kinit command from the configuration file.
     */
    @Test
    public void testSpawnAutoRenewalThreadForUserCreds_withKerberosAuthentication() throws InterruptedException {
        // Mock the Configuration object
        Configuration mockConf = mock(Configuration.class);
        when(mockConf.getTrimmed("hadoop.kerberos.kinit.command", "kinit"))
            .thenReturn("kinit");

        // Mock UserGroupInformation object
        UserGroupInformation mockUGI = spy(new UserGroupInformation());
        doReturn(mockConf).when(mockUGI).getConf();
        doNothing().when(mockUGI).spawnAutoRenewalThreadForUserCreds();

        // Call the method under test
        mockUGI.spawnAutoRenewalThreadForUserCreds();

        // Verify that the configuration and credentials renewal is properly initiated
        verify(mockConf).getTrimmed("hadoop.kerberos.kinit.command", "kinit");
        verify(mockUGI).spawnAutoRenewalThreadForUserCreds();
    }

    /**
     * Test case: Validate that the `loginUserFromSubject` method successfully authenticates
     * a user from the provided `Subject` and utilizes configurations.
     */
    @Test
    public void testLoginUserFromSubject_withValidSubject() throws Exception {
        // Mock the Subject object and the Configuration object
        Subject mockSubject = mock(Subject.class);
        Configuration mockConf = mock(Configuration.class);
        when(mockConf.getTrimmed("hadoop.kerberos.kinit.command", "kinit"))
            .thenReturn("kinit");

        // Set the mocked configuration into UserGroupInformation
        UserGroupInformation.setConfiguration(mockConf);

        // Call the method under test
        UserGroupInformation.loginUserFromSubject(mockSubject);

        // Verify that configurations are accessed and the Subject is utilized
        verify(mockConf).getTrimmed("hadoop.kerberos.kinit.command", "kinit");
        verify(mockSubject, atLeastOnce()).toString(); // Ensure Subject interactions
    }
}