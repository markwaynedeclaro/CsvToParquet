package org.example.s3ToParquetFilter.model;

import lombok.Getter;
import lombok.Setter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Setter
@Getter
@Component
public class AWSCredential {

    @Value("${key.id}")
    private String keyId;

    @Value("${access.key}")
    private String accessKey;

    @Value("${region}")
    private String region;

    @Value("${bucket.name}")
    private String bucketName;

}
