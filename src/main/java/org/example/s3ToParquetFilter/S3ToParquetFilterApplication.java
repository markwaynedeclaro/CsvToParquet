package org.example.s3ToParquetFilter;


import lombok.extern.log4j.Log4j2;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * run config : mvn spring-boot:run -Dspring-boot.run.arguments="--key.id=<YOUR_ACCESS_KEY_ID> --access.key=<YOUR_SECRET_ACCESS_KEY> --bucket.name=<YOUR_BUCKET_NAME> --region=<YOUR_REGION>"
 *
 * @author Mark Wayne de Claro
 * @version 1.0 10/08/2021
 */
@Log4j2
@SpringBootApplication
public class S3ToParquetFilterApplication implements CommandLineRunner {

    public static void main(String[] args) {
        SpringApplication.run(S3ToParquetFilterApplication.class, args);
    }

    @Override
    public void run(String... args) throws Exception {}

}
