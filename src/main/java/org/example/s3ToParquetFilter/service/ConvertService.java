package org.example.s3ToParquetFilter.service;

import org.example.s3ToParquetFilter.exception.ConversionException;
import org.example.s3ToParquetFilter.exception.FileException;

import java.util.List;

/**
 * This interface handles the conversion processes
 */
public interface ConvertService {

    /**
     * handles the conversion of a file
     * @return the converted file
     * @throws ConversionException
     */
    void convertFiles(final List<String> inputFileNames) throws ConversionException, FileException;

}
