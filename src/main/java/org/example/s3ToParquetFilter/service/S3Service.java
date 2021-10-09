package org.example.s3ToParquetFilter.service;

import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.io.FileUtils;
import org.example.s3ToParquetFilter.exception.DataSourceException;
import org.springframework.stereotype.Service;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import com.amazonaws.services.s3.model.S3Object;


@Log4j2
@Service
public class S3Service {

    /**
     * Get S3 connection
     * @param accessKey
     * @param secretKey
     * @param region
     * @return
     * @throws DataSourceException
     */
    public static AmazonS3 getS3Client(final String accessKey, final String secretKey, final String region) throws DataSourceException {
        try {
            final BasicAWSCredentials basicAWSCredentials = new BasicAWSCredentials(accessKey, secretKey);
            return AmazonS3ClientBuilder.standard()
                    .withRegion(Regions.fromName(region))
                    .withCredentials(new AWSStaticCredentialsProvider(basicAWSCredentials))
                    .build();
        } catch (final Exception e) {
            log.error("Error encountered while getting S3 Client.", e);
            throw new DataSourceException("Error while getting S3 Client " + e.getMessage());
        }
    }


    /**
     * Uploads a file into the bucket
     * @param s3Client
     * @param bucketName
     * @param filename
     * @param file
     * @throws DataSourceException
     */
    public static void uploadFileToBucket(final AmazonS3 s3Client, final String bucketName, final String filename, final File file) throws DataSourceException {
        try {
            s3Client.putObject(bucketName, filename, file);
        } catch (final Exception e) {
            log.error("Error encountered while uploading file to bucket.", e);
            throw new DataSourceException("Error while uploading file to bucket " + e.getMessage());
        }
    }


    /**
     * Downloads a file into the download path provided
     * @param s3Client
     * @param bucketName
     * @param filePath
     * @param downloadPath
     * @throws DataSourceException
     */
    public static void downloadFileFromBucket(final AmazonS3 s3Client, final String bucketName, final String filePath, final String downloadPath) throws DataSourceException {
        try {
            final S3Object obj = s3Client.getObject(bucketName, filePath);
            if (null != obj) {
                final InputStream is = obj.getObjectContent();
                if (null != is) {
                    final File tempFile = new File(downloadPath);
                    try {
                        FileUtils.copyInputStreamToFile(is, tempFile);
                    } catch (final IOException e) {
                        log.error("Error encountered while downloading file from bucket.", e);
                    }
                }
            }
        } catch (final Exception e) {
            log.error("Error encountered while downloading file from bucket.", e);
            throw new DataSourceException("Error while downloading file from bucket " + e.getMessage());
        }
    }

    /**
     * Returns list of files inside the bucket
     * @param s3Client
     * @param bucketId
     * @param remotePath
     * @return
     * @throws Exception
     */
    public static List<String> listAllFilesFromBucketWithRemotePath(final AmazonS3 s3Client, final String bucketId, final String remotePath) throws DataSourceException {
        List<String> fileList = new ArrayList<>();
        try {
            final ObjectListing objects = s3Client.listObjects(new ListObjectsRequest(bucketId, remotePath, null, null, null));
            if (null != objects) {
                final List<S3ObjectSummary> objectSummaries = objects.getObjectSummaries();
                if (CollectionUtils.isNotEmpty(objectSummaries)) {
                    fileList = new ArrayList<>();
                    for (final S3ObjectSummary s3Obj : objectSummaries) {
                        fileList.add(s3Obj.getKey());
                    }
                }
            }
        } catch (Exception e) {
            throw new DataSourceException("Error while listing all files from Bucket " + e.getMessage());
        }
        return fileList;
    }

}
