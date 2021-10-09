package org.example.s3ToParquetFilter.config;

import lombok.extern.log4j.Log4j2;
import org.example.s3ToParquetFilter.job.ConvertJob;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;

/**
 * This controls the timing of running the converter
 */
@Log4j2
@Configuration
@EnableScheduling
public class PollingTimerConfig {

    @Autowired
    private ConvertJob convertJob;

    @Scheduled(fixedDelayString = "${runIntervalInMilliseconds}", initialDelayString = "${runIntervalInMilliseconds.delay}")
    public void start() {
        try {
            convertJob.convertToApacheParquetFormat();
        } catch (final Exception e) {
            log.error("Error encountered while converting to parquet format.", e);
        }
    }

}
