package org.example.s3ToParquetFilter.service.impl;

import lombok.extern.log4j.Log4j2;
import org.example.s3ToParquetFilter.exception.ConversionException;
import org.example.s3ToParquetFilter.service.ConvertService;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;


@Log4j2
@Service("parquet")
public class ConvertToParquetService implements ConvertService {

    /**
     * handles the conversion of a file into Parquet format
     *
     * @param inputFileNames list of filenames
     * @return the coverted filenames
     * @throws ConversionException
     */
    @Override
    public void convertFiles(final List<String> inputFileNames) throws ConversionException {

        try {
            final ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(inputFileNames.size());

            final CountDownLatch latch = new CountDownLatch(inputFileNames.size());

            final List<Future<String>> resultList = new ArrayList<>();

            for (String filename : inputFileNames) {
                final ParquetConverter converter = new ParquetConverter(filename, latch);
                final Future<String> result = executor.submit(converter);
                resultList.add(result);
            }

            //shut down the executor service
            executor.shutdown();

            try {
                latch.await();
            } catch (final InterruptedException ex) {
                log.error("Error encountered while converting files.", ex);
            }

        } catch (final Exception e) {
            log.error("Error encountered while converting files.", e);
            throw new ConversionException("Error during file conversion. " + e.getMessage());
        }
    }


}
