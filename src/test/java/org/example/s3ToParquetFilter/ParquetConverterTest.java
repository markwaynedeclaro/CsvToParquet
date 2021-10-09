package org.example.s3ToParquetFilter;

import lombok.extern.log4j.Log4j2;
import org.example.s3ToParquetFilter.service.FileManagementService;
import org.example.s3ToParquetFilter.service.impl.ParquetConverter;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Profile;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.File;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;

import static org.junit.Assert.assertEquals;
import static org.example.s3ToParquetFilter.config.Resources.MAIN_RB;

@Log4j2
@RunWith(SpringRunner.class)
@Profile("test")
@SpringBootTest()
public class ParquetConverterTest {

    @Autowired
    private FileManagementService fileManagementService;

    @Before
    public void setUp() throws Exception {
        fileManagementService.deleteDirectory(MAIN_RB.get("input.folder.download"));
        fileManagementService.createDirectory(MAIN_RB.get("input.folder.download"));
        fileManagementService.createDirectory(MAIN_RB.get("output.folder.temp"));
    }

    @After
    public void tearDown() throws Exception {
        //fileManagementService.deleteDirectory(MAIN_RB.get("input.folder.download"));
    }

    @Test
    public void testCsvFileWithHeader() {
        try {
            String filename = MAIN_RB.get("input.file1");
            ParquetConverter converter = new ParquetConverter(filename);
            converter.createSchemaFile(MAIN_RB.get("output.schema.file1"));
            converter.filterToNewCsvFile(MAIN_RB.get("output.csv.file1"));

            File newSchemaFile = new File(MAIN_RB.get("output.schema.file1"));
            File newCsvFile = new File(MAIN_RB.get("output.csv.file1"));

            assertEquals("Size of new CSV File (with Header) ",
                    2,
                    Files.lines(newCsvFile.toPath(),StandardCharsets.UTF_8).count());
            assertEquals("Schema Content (with Header) ",
                    "message csv {required binary name = 1;required binary real_age = 2;required binary favorite_food = 3;}",
                    com.google.common.io.Files.readFirstLine(newSchemaFile, Charset.defaultCharset()));

        } catch (final Exception e) {
            log.error("Error during testing of Csv File With Header", e);
        }
    }

    @Test
    public void testCsvFileWithoutHeader() {
        try {
            String filename = MAIN_RB.get("input.file2");
            ParquetConverter converter = new ParquetConverter(filename);
            converter.createSchemaFile(MAIN_RB.get("output.schema.file2"));
            converter.filterToNewCsvFile(MAIN_RB.get("output.csv.file2"));

            File newSchemaFile = new File(MAIN_RB.get("output.schema.file2"));
            File newCsvFile = new File(MAIN_RB.get("output.csv.file2"));

            assertEquals("Size of new CSV File (with Header) ",
                    2,
                    Files.lines(newCsvFile.toPath(),StandardCharsets.UTF_8).count());
            assertEquals("Schema Content (with Header) ",
                    "message csv {required binary field_1 = 1;required binary field_2 = 2;required binary field_3 = 3;}",
                    com.google.common.io.Files.readFirstLine(newSchemaFile, Charset.defaultCharset()));

        } catch (final Exception e) {
            log.error("Error during testing of Csv File Without Header", e);
        }
    }
}
