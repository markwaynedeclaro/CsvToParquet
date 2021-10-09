package org.example.s3ToParquetFilter.service.impl;

import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.MessageType;

import java.io.IOException;


import java.util.List;

public class CsvParquetWriter extends ParquetWriter<List<String>> {

    public CsvParquetWriter(final Path file, final MessageType schema, final boolean enableDictionary) throws IOException {
        this(file, schema, CompressionCodecName.UNCOMPRESSED, enableDictionary);
    }

    public CsvParquetWriter(final Path file, final MessageType schema, final CompressionCodecName codecName, final boolean enableDictionary) throws IOException {
        super(file, (WriteSupport<List<String>>) new CsvWriteSupport(schema), codecName, DEFAULT_BLOCK_SIZE, DEFAULT_PAGE_SIZE, enableDictionary, false);
    }

}

