package org.apache.hadoop.net;   

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;
import java.io.File;
import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Objects;

import static org.junit.Assert.*;

public class TestConfigurationValidation {       
    @Test
    public void testNetTopologyTableMappingFileConfiguration() {
        // Load hadoop-common configuration
        Configuration configuration = new Configuration();

        // Check the value of net.topology.node.switch.mapping.impl
        String nodeSwitchMappingImpl = configuration.get("net.topology.node.switch.mapping.impl");
        assertNotNull("net.topology.node.switch.mapping.impl is not configured.", nodeSwitchMappingImpl);

        // If net.topology.node.switch.mapping.impl is set to org.apache.hadoop.net.TableMapping,
        // validate net.topology.table.file.name
        if (Objects.equals(nodeSwitchMappingImpl, "org.apache.hadoop.net.TableMapping")) {
            String topologyTableFileName = configuration.get("net.topology.table.file.name", null);

            // Ensure net.topology.table.file.name is not blank
            assertNotNull("net.topology.table.file.name is not configured when using TableMapping.", topologyTableFileName);
            assertFalse("net.topology.table.file.name is blank.", topologyTableFileName.trim().isEmpty());

            // Validate topology file path existence
            File topologyFile = new File(topologyTableFileName);
            assertTrue("Topology file specified in net.topology.table.file.name does not exist or is not accessible.",
                    topologyFile.exists() && topologyFile.isFile());

            // Optionally, validate file content structure
            // This test only ensures the basic condition of file format correctness
            validateTopologyFileStructure(topologyFile);
        }
    }

    private void validateTopologyFileStructure(File topologyFile) {
        try (BufferedReader reader = new BufferedReader(new FileReader(topologyFile))) {
            String line;
            while ((line = reader.readLine()) != null) {
                line = line.trim();
                // Ignore comments and blank lines
                if (line.length() == 0 || line.charAt(0) == '#') {
                    continue;
                }

                // Validate two-column format
                String[] columns = line.split("\\s+");
                assertEquals("Each line in the topology file must contain exactly two columns.",
                        2, columns.length);
            }
        } catch (Exception e) {
            fail("Failed to correctly parse or validate topology file: " + e.getMessage());
        }
    }
}