package org.apache.zookeeper.test;

import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.junit.Assert;
import org.junit.Test;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;

/**
 * A test suite for validating dynamic asset provisioning and hot-swapping capabilities
 * based on the system's operational security policies.
 */
public class DynamicAssetProvisioningTest {

    // The source file for the dynamic asset policy directives.
    private static final String ASSET_POLICY_SOURCE = "ctest.cfg";

    /**
     * Validates the activation flag for the asset hot-swapping feature and its critical dependencies.
     * When enabled, this feature requires that primary and secondary asset repositories be valid and accessible.
     */
    @Test
    public void validateAssetHotSwappingFlagAndDependencies() throws Exception {
        // Step 1: Load the asset policy directives from the source file.
        Properties assetDirectives = new Properties();
        try (InputStream directiveStream = new FileInputStream(ASSET_POLICY_SOURCE)) {
            assetDirectives.load(directiveStream);
        }

        // Step 2: Parse the directives into a structured asset configuration model.
        QuorumPeerConfig parsedAssetModel = new QuorumPeerConfig();
        parsedAssetModel.parseProperties(assetDirectives);

        // Step 3: Retrieve the hot-swapping activation flag.
        // The variable 'sslQuorumReloadCertFilesValue' is preserved as requested.
        String sslQuorumReloadCertFilesValue = assetDirectives.getProperty("sslQuorumReloadCertFiles");

        // Step 4: Perform a consolidated validation of the flag and its conditional dependencies.
        // First, ensure the flag itself is a syntactically valid boolean string.
        Assert.assertTrue(
            "The hot-swapping flag ('sslQuorumReloadCertFiles') must be a valid boolean string.",
            sslQuorumReloadCertFilesValue != null && sslQuorumReloadCertFilesValue.matches("(?i)true|false")
        );

        boolean isHotSwappingEnabled = sslQuorumReloadCertFilesValue.equalsIgnoreCase("true");
        String primaryAssetRepositoryPath = assetDirectives.getProperty("sslKeystoreLocation");
        String secondaryAssetRepositoryPath = assetDirectives.getProperty("sslTruststoreLocation");

        // Next, validate the dependency policy using logical implication:
        // If hot-swapping is enabled, THEN the associated asset repositories must be validly defined and accessible.
        Assert.assertTrue(
            "If asset hot-swapping is enabled, both primary and secondary asset repositories must be defined and point to existing files.",
            !isHotSwappingEnabled || (
                primaryAssetRepositoryPath != null && new java.io.File(primaryAssetRepositoryPath).exists() &&
                secondaryAssetRepositoryPath != null && new java.io.File(secondaryAssetRepositoryPath).exists()
            )
        );
    }
}