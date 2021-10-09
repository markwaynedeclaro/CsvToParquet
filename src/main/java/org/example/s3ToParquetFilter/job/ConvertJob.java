package org.example.s3ToParquetFilter.job;

import com.amazonaws.services.s3.AmazonS3;
import lombok.extern.log4j.Log4j2;
import org.example.s3ToParquetFilter.model.AWSCredential;
import org.example.s3ToParquetFilter.service.ConvertService;
import org.example.s3ToParquetFilter.service.FileManagementService;
import org.example.s3ToParquetFilter.service.S3Service;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import java.io.File;

import static org.example.s3ToParquetFilter.config.Resources.MAIN_RB;

@Log4j2
@Component
public class ConvertJob {

    @Autowired
    private AWSCredential awsCredential;

    @Autowired
    private FileManagementService fileManagementService;

    @Autowired
    @Qualifier("parquet")
    private ConvertService convertService;

    public void convertToApacheParquetFormat() throws Exception {

        final String downloadPath = MAIN_RB.get("input.folder.download");
        final String inputFile = MAIN_RB.get("input.file");
        final String newFilePath = downloadPath + "/" + inputFile;
        final String tempPath = MAIN_RB.get("input.folder.download.temp");
        final String outputPath = MAIN_RB.get("output.folder");
        final String outputZipFilePath = MAIN_RB.get("output.folder.zip");
        final String outputTempPath = MAIN_RB.get("output.folder.temp");
        final String s3OutputZip = MAIN_RB.get("s3.output.zip");

        log.info("Step 1 of 7 --- Cleanup/Clear Job directory ");
        fileManagementService.deleteDirectory(downloadPath);
        fileManagementService.createDirectory(downloadPath);
        fileManagementService.createDirectory(outputPath);
        fileManagementService.createDirectory(outputTempPath);

        log.info("Step 2 of 7 --- Download input file from S3 ");
        final AmazonS3 s3Client = S3Service.getS3Client(awsCredential.getKeyId(), awsCredential.getAccessKey(), awsCredential.getRegion());
        S3Service.downloadFileFromBucket(s3Client, awsCredential.getBucketName(), inputFile, newFilePath);

        log.info("Step 3 of 7 --- Decompress input file");
        fileManagementService.decompressFile(newFilePath, tempPath);

        log.info("Step 4 of 7 --- Convert CSV Files to Parquet ");
        convertService.convertFiles(fileManagementService.getFileList(tempPath));

        log.info("Step 5 of 7 --- Compress output to a zip file ");
        final File outputZip = fileManagementService.compressFolder(outputPath, outputZipFilePath);

        log.info("Step 6 of 7 --- Upload output file to S3 ");
        S3Service.uploadFileToBucket(s3Client, awsCredential.getBucketName(), s3OutputZip, outputZip);

        log.info("Step 7 of 7 --- Cleanup Job directory ");
        fileManagementService.deleteDirectory(downloadPath);

        log.info("--- Job Completed---");

    }

}
