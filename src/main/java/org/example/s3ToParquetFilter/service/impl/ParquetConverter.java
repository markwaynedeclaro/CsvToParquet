package org.example.s3ToParquetFilter.service.impl;

import lombok.extern.log4j.Log4j2;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;

import java.io.*;

import org.apache.commons.lang3.math.NumberUtils;
import org.apache.hadoop.fs.Path;

import com.google.common.io.Files;

import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.regex.Pattern;

import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.example.s3ToParquetFilter.exception.ConversionException;
import org.example.s3ToParquetFilter.exception.FileException;
import org.example.s3ToParquetFilter.service.FileManagementService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import static org.example.s3ToParquetFilter.config.Resources.MAIN_RB;

/**
 * Callable - for multithreaded conversion to parquet
 */
@Log4j2
public class ParquetConverter implements Callable<String> {

    private String filePath;
    private String CSV_SEPARATOR;
    private CountDownLatch latch;
    private FileManagementService fileManagementService;


    public ParquetConverter(String filePath, CountDownLatch latch) {
        this.filePath = filePath;
        this.latch = latch;
        CSV_SEPARATOR = MAIN_RB.get("csv.separator");
        fileManagementService = new FileManagementService();
    }

    public ParquetConverter(String filePath) {
        this.filePath = filePath;
        CSV_SEPARATOR = MAIN_RB.get("csv.separator");
        fileManagementService = new FileManagementService();
    }

    /**
     * Computes a result, or throws an exception if unable to do so.
     *
     * @return computed result
     * @throws Exception if unable to compute a result
     */
    @Override
    public String call() throws Exception {

        final File originalCsv = new File(filePath);
        final String newCsvFilePath = MAIN_RB.get("output.folder.temp") + "/" + originalCsv.getName();
        final String schemaFilePath = MAIN_RB.get("output.folder.temp") + "/"
                + fileManagementService.removeFileExtension(originalCsv.getName(), true)
                + MAIN_RB.get("file.extension.schema");
        final String outputParquetFilePath = MAIN_RB.get("output.folder") + "/"
                + fileManagementService.removeFileExtension(originalCsv.getName(), true)
                + MAIN_RB.get("file.extension.parquet");
        final File outputParquetFile = new File(outputParquetFilePath);

        log.info("Start a new Thread for "+filePath);
        createSchemaFile(schemaFilePath);
        filterToNewCsvFile(newCsvFilePath);
        convertCsvToParquet(schemaFilePath, new File(newCsvFilePath), outputParquetFile, false);


        this.latch.countDown();

        return outputParquetFilePath;
    }


    /**
     * filters the original CSV file then creates a new one
     *
     * @param newFilePath path of the new CSV file
     * @throws Exception
     */
    public void filterToNewCsvFile(final String newFilePath) throws Exception {

        final String ls = System.getProperty("line.separator");
        final Pattern p = Pattern.compile(MAIN_RB.get("pattern"), Pattern.CASE_INSENSITIVE);
        final LineIterator it = FileUtils.lineIterator(new File(filePath), "UTF-8");

        try (PrintWriter writer = new PrintWriter(new File(newFilePath))) {
            while (it.hasNext()) {
                final String line = it.nextLine();
                if (p.matcher(line).find()) {
                    writer.write(line + ls);
                }
            }
        } finally {
            it.close();
        }
    }

    /**
     * Schema file generator
     * Creates defualt schema if any of the column on the first row of a csv file is numeric or has a space
     *
     * @param newFilePath schema file filepath
     * @throws Exception
     */
    public void createSchemaFile(final String newFilePath) throws Exception {
        final String firstLine = Files.readFirstLine(new File(filePath), Charset.defaultCharset());
        boolean needsDefaultSchema = false;
        final String[] columns = firstLine.split(CSV_SEPARATOR);
        for (String col : columns) {
            if (NumberUtils.isParsable(col) || col.trim().contains(" ")) {
                needsDefaultSchema = true;
                break;
            }
        }

        if (needsDefaultSchema) {
            try (PrintWriter writer = new PrintWriter(new File(newFilePath))) {
                writer.write(createDefaultHeader(columns.length));
            } catch (final Exception e) {
                log.error("Error encountered while creating schema.", e);
                throw new ConversionException("Error during schema creation. " + e.getMessage());
            }
        } else {
            try (PrintWriter writer = new PrintWriter(new File(newFilePath))) {
                writer.write(convertHeader(firstLine));
            } catch (final Exception e) {
                log.error("Error encountered while creating schema.", e);
                throw new ConversionException("Error during schema creation. " + e.getMessage());
            }
        }
    }


    /**
     * default header generator
     *
     * @param size number of columns
     * @throws Exception
     */
    private String createDefaultHeader(int size) throws Exception {
        StringBuilder sb = new StringBuilder();
        final String messageFieldOpening = MAIN_RB.get("message.field.opening.default");
        final String messageFieldClosing = MAIN_RB.get("message.field.closing");

        sb.append(MAIN_RB.get("message.opening"));
        for (int i = 1; i <= size; i++) {
            sb.append(messageFieldOpening);
            sb.append(i + " = " + i);
            sb.append(messageFieldClosing);
        }
        sb.append(MAIN_RB.get("message.closing"));

        return sb.toString();
    }

    /**
     * header generator
     *
     * @param header original header
     * @throws Exception
     */
    private String convertHeader(String header) throws Exception {
        StringBuilder sb = new StringBuilder();
        final String messageFieldOpening = MAIN_RB.get("message.field.opening");
        final String messageFieldClosing = MAIN_RB.get("message.field.closing");

        String[] columns = header.split(CSV_SEPARATOR, -1);
        sb.append(MAIN_RB.get("message.opening"));
        for (int i = 1; i <= columns.length; i++) {
            sb.append(messageFieldOpening);
            sb.append(" " + columns[i - 1] + " = " + i);
            sb.append(messageFieldClosing);
        }
        sb.append(MAIN_RB.get("message.closing"));

        return sb.toString();
    }

    /**
     * @param csvFile           the new CSV input file
     * @param outputParquetFile the new parquet file
     * @param enableDictionary
     * @throws IOException
     */
    public void convertCsvToParquet(String schemaPath, File csvFile, File outputParquetFile, boolean enableDictionary) throws IOException, FileException {
        String rawSchema = fileManagementService.readFile(schemaPath);
        Path path = new Path(outputParquetFile.toURI());
        MessageType schema = MessageTypeParser.parseMessageType(rawSchema);
        CsvParquetWriter writer = new CsvParquetWriter(path, schema, enableDictionary);

        BufferedReader br = new BufferedReader(new FileReader(csvFile));
        String line;
        try {
            while ((line = br.readLine()) != null) {
                writer.write(Arrays.asList(line.split(CSV_SEPARATOR)));
            }
            writer.close();
        } finally {
            br.close();
        }
    }

}
