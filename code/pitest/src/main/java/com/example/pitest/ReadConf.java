package com.example.pitest;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.BufferedReader;
import java.io.FileReader;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.regex.Matcher;
import org.apache.poi.ss.usermodel.*;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;


public class ReadConf {



    private static String filePath = "resources/hdfs.xlsx";

    private static String methodNamesFile = "method_names.txt";

    private static final Set<String> methodNames = new LinkedHashSet<>();

    private static String extractMethodNames(String text) {
        Pattern pattern = Pattern.compile("(\\w+)(?=\\()");
        Matcher matcher = pattern.matcher(text);
        if (matcher.find()) {
            return matcher.group(1);
        }
        return null;
    }


    private static void readExcel(String filePath) {
        File file = new File(filePath);
        if (!file.exists()) {
            return;
        }

        try (FileInputStream fis = new FileInputStream(file);
             Workbook workbook = new XSSFWorkbook(fis)) {

            Sheet sheet = workbook.getSheetAt(0);
            DataFormatter formatter = new DataFormatter();

            for (Row row : sheet) {
                if (row == null) continue;
                int lastCol = row.getLastCellNum();
                for (int cn = 0; cn < lastCol; cn++) {
                    Cell cell = row.getCell(cn, Row.MissingCellPolicy.CREATE_NULL_AS_BLANK);
                    String text = formatter.formatCellValue(cell);
                    String m = extractMethodNames(text);
                    if (m != null) {
                        methodNames.add(m);
                    }
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    private static void saveMethodNamesToFile(Set<String> names, String outFile) {
        try (FileWriter writer = new FileWriter(outFile)) {
            for (String name : names) {
                writer.write(name + "\n");
            }

        } catch (IOException e) {
            System.err.println( e.getMessage());
        }
    }
    

    private static Set<String> loadMethodNamesFromFile(String inFile) {
        Set<String> names = new LinkedHashSet<>();
        File file = new File(inFile);
        if (!file.exists()) {
            return null;
        }
        
        try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
            String line;
            while ((line = reader.readLine()) != null) {
                line = line.trim();
                if (!line.isEmpty()) {
                    names.add(line);
                }
            }
        } catch (IOException e) {
            System.err.println(e.getMessage());
        }
        return names;
    }
    

    public static Set<String> getMethodNames(String excelPath, String namesFile) {

        Set<String> names = loadMethodNamesFromFile(namesFile);
        if (names != null && !names.isEmpty()) {
            return names;
        }
        
        methodNames.clear();
        readExcel(excelPath);
        if (!methodNames.isEmpty()) {
            saveMethodNamesToFile(methodNames, namesFile);
        }
        return methodNames;
    }
    
    public static void main(String[] args) {
        if (args.length >= 1) {
            filePath = args[0];
        }
        if (args.length >= 2) {
            methodNamesFile = args[1];
        }
        
        Set<String> names = getMethodNames(filePath, methodNamesFile);
        for (String name : names) {
            System.out.println(name);
        }
    }
}
