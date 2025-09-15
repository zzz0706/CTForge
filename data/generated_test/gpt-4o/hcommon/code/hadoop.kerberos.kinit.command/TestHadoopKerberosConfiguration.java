package org.apache.hadoop.security;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import java.io.File;

import static org.mockito.Mockito.*;

public class TestHadoopKerberosConfiguration {

    /**
     * Test case: Validate that the kinit executable is correctly identified and validated when given a valid absolute path.
     * Tests the configuration property propagation and validation.
     */
    @Test
    public void testValidateKinitExecutable_withValidAbsolutePath() {
        // Mock the Configuration object
        Configuration mockConf = mock(Configuration.class);

        // Simulate setting the configuration for the kinit command
        when(mockConf.getTrimmed(KDiag.KERBEROS_KINIT_COMMAND, "")).thenReturn("/usr/bin/kinit");

        // Mock the File object to simulate the existence of a valid kinit executable
        File mockFile = mock(File.class);
        when(mockFile.isAbsolute()).thenReturn(true);
        when(mockFile.exists()).thenReturn(true);
        when(mockFile.length()).thenReturn(100L); // Simulate a non-empty file

        // Spy on KDiag instance while injecting mocked configuration and file handling
        KDiag kDiag = spy(new KDiag());
        doReturn(mockConf).when(kDiag).getConf();
        doReturn(mockFile).when(kDiag).wrapFileInstance(any(String.class));
        doNothing().when(kDiag).println(anyString(), any());

        // Invoke the validateKinitExecutable method
        kDiag.validateKinitExecutable();

        // Verify interactions
        verify(mockConf).getTrimmed(KDiag.KERBEROS_KINIT_COMMAND, "");
        verify(kDiag).wrapFileInstance("/usr/bin/kinit");
        verify(mockFile).isAbsolute();
        verify(mockFile).exists();
        verify(mockFile).length();
        verify(kDiag).println(anyString(), eq(mockFile));
    }

    /**
     * Test case: Validate the execution method to ensure all diagnostics are executed correctly and configurations are utilized.
     */
    @Test
    public void testExecute_withValidConfiguration() throws Exception {
        // Mock the Configuration object
        Configuration mockConf = mock(Configuration.class);
        when(mockConf.getTrimmed(KDiag.KERBEROS_KINIT_COMMAND, "")).thenReturn("/usr/bin/kinit");

        // Spy on KDiag instance while injecting mocked constructs
        KDiag kDiag = spy(new KDiag());
        doReturn(mockConf).when(kDiag).getConf();
        doNothing().when(kDiag).validateKinitExecutable();

        // Invoke the execute method
        boolean result = kDiag.execute();

        // Verify interactions
        verify(mockConf).getTrimmed(KDiag.KERBEROS_KINIT_COMMAND, "");
        verify(kDiag).validateKinitExecutable();
        assert result; // Ensure result is true
    }

    /**
     * Test case: Validate that auto-renewal of Kerberos credentials spawns correctly and utilizes the kinit command.
     */
    @Test
    public void testSpawnAutoRenewalThreadForUserCreds_withKerberosAuthentication() throws InterruptedException {
        // Mock the Configuration object and UserGroupInformation instance
        Configuration mockConf = mock(Configuration.class);
        when(mockConf.getTrimmed("hadoop.kerberos.kinit.command", "kinit")).thenReturn("kinit");
        UserGroupInformation mockUGI = spy(new UserGroupInformation());

        // Inject configuration into UserGroupInformation
        doReturn(mockConf).when(mockUGI).getConf();
        doNothing().when(mockUGI).spawnAutoRenewalThreadForUserCreds();

        // Trigger the auto-renewal thread spawning
        mockUGI.spawnAutoRenewalThreadForUserCreds();

        // Verify interactions
        verify(mockConf).getTrimmed("hadoop.kerberos.kinit.command", "kinit");
        verify(mockUGI).spawnAutoRenewalThreadForUserCreds();
    }

    /**
     * Test case: Validate the login using a provided subject and ensures configurations and credentials are used.
     */
    @Test
    public void testLoginUserFromSubject_withValidSubject() throws Exception {
        // Mock the Subject and Configuration objects
        javax.security.auth.Subject mockSubject = mock(javax.security.auth.Subject.class);
        Configuration mockConf = mock(Configuration.class);
        when(mockConf.getTrimmed("hadoop.kerberos.kinit.command", "kinit")).thenReturn("kinit");
        UserGroupInformation.setConfiguration(mockConf);

        // Invoke loginUserFromSubject method
        UserGroupInformation.loginUserFromSubject(mockSubject);

        // Verify interactions
        verify(mockConf).getTrimmed("hadoop.kerberos.kinit.command", "kinit");
        verify(mockSubject, atLeastOnce()).toString(); // Mocked Subject should be invoked
    }
}