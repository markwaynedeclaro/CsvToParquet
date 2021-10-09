package org.example.s3ToParquetFilter.service;

import lombok.extern.log4j.Log4j2;
import net.lingala.zip4j.ZipFile;
import org.apache.commons.io.FileUtils;
import org.example.s3ToParquetFilter.exception.FileException;
import org.springframework.stereotype.Service;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;


/**
 * This handles file related activities
 */
@Log4j2
@Service
public class FileManagementService {

    /**
     * Creates the desired directory
     * @param path the path of the directory
     * @throws FileException
     */
    public void createDirectory(final String path) throws FileException {
        try {
            final File directory = new File(path);
            if (!directory.exists()) {
                directory.mkdir();
            }
        } catch (final Exception e) {
            log.error("Error encountered while creating directory.", e);
            throw new FileException("Error during directory creation. " + e.getMessage());
        }
    }

    /**
     * deletes the desired directory
     * @param path the path of the file
     * @throws FileException
     */
    public void deleteDirectory(final String path) throws FileException {
        try {
            final File directory = new File(path);
            if (directory.exists()) {
                FileUtils.deleteDirectory(directory);
            }
        } catch (final Exception e) {
            log.error("Error encountered while deleting directory.", e);
            throw new FileException("Error during directory deletion. " + e.getMessage());
        }
    }

    /**
     * Retrieves the list of filenames inside the directory
     * @param directoryPath path of the directory
     * @return list of filenames inside the directory
     * @throws FileException
     */
    public List<String> getFileList(final String directoryPath) throws FileException {
        final List<String> filenames = new ArrayList<>();
        try {
            final File dir = new File(directoryPath);
            if(dir.isDirectory()) {
                for(File file : dir.listFiles()) {
                    filenames.add(file.getPath());
                }
            }
        } catch (final Exception e) {
            log.error("Error encountered while getting file list.", e);
            throw new FileException("Error while fetching files from a directory. " + e.getMessage());
        }
        return filenames;
    }

    /**
     * Decompresses a ZIP file into a destination folder
     * @param filePath the path of the ZIP file
     * @param destinationPath the path where to unzip the file
     * @throws FileException
     */
    public void decompressFile(final String filePath, final String destinationPath) throws FileException {
        try {
            this.createDirectory(destinationPath);
            final File destDir = new File(destinationPath);
            final byte[] buffer = new byte[1024];
            final ZipInputStream zis = new ZipInputStream(new FileInputStream(filePath));
            ZipEntry zipEntry = zis.getNextEntry();

            while (zipEntry != null) {
                final File newFile = newFile(destDir, zipEntry);
                final FileOutputStream fos = new FileOutputStream(newFile);
                int len;
                while ((len = zis.read(buffer)) > 0) {
                    fos.write(buffer, 0, len);
                }
                fos.close();
                zipEntry = zis.getNextEntry();
            }
            zis.closeEntry();
            zis.close();

        } catch (final IOException e) {
            log.error("Error encountered while decompressing file.", e);
            throw new FileException("Error during file decompression. " + e.getMessage());
        }
    }

    /**
     * Compresses a folder into a ZIP file
     * @param folderPath folder to be compressed
     * @param pathZipFile the path of the zip file to be created
     * @throws IOException
     */
    public File compressFolder(final String folderPath, final String pathZipFile) throws FileException {
        try {
            final ZipFile zipFile = new ZipFile(pathZipFile);

            final List<File> files = Files.walk(Paths.get(folderPath))
                    .filter(Files::isRegularFile)
                    .map(Path::toFile)
                    .collect(Collectors.toList());

            zipFile.addFiles(files);

            return zipFile.getFile();

        } catch (final IOException e) {
            log.error("Error encountered while compressing folder.", e);
            throw new FileException("Error during folder compression. " + e.getMessage());
        } catch (Exception e) {
            log.error("Error encountered while compressing folder.", e);
            throw new FileException("Error during folder compression. " + e.getMessage());
        }
    }


    /**
     * This method guards against writing files to the file system outside of the target folder.
     * @param destinationDir the destination path/file
     * @param zipEntry
     * @return
     * @throws IOException
     */
    public static File newFile(final File destinationDir, final ZipEntry zipEntry) throws IOException {
        final File destinationFile = new File(destinationDir, zipEntry.getName());
        final String destinationDirPath = destinationDir.getCanonicalPath();
        final String destinationFilePath = destinationFile.getCanonicalPath();

        if (!destinationFilePath.startsWith(destinationDirPath + File.separator)) {
            throw new IOException("Entry is outside of the target dir: " + zipEntry.getName());
        }

        return destinationFile;
    }

    /**
     * Removes the file extension from a filename
     * @param filename
     * @param removeAllExtensions
     * @return
     */
    public String removeFileExtension(final String filename, final boolean removeAllExtensions) {
        if (filename == null || filename.isEmpty()) {
            return filename;
        }

        final String extPattern = "(?<!^)[.]" + (removeAllExtensions ? ".*" : "[^.]*$");
        return filename.replaceAll(extPattern, "");
    }

    /**
     * Reads a file and converts the content into a String
     * @param path
     * @return
     * @throws IOException
     */
    public String readFile(final String path) throws IOException, FileException  {
        final BufferedReader reader = new BufferedReader(new FileReader(path));
        final StringBuilder stringBuilder = new StringBuilder();

        try {
            String line = null;
            final String ls = System.getProperty("line.separator");

            while ((line = reader.readLine()) != null) {
                stringBuilder.append(line);
                stringBuilder.append(ls);
            }
        } catch (final Exception e) {
            log.error("Error encountered while reading file.", e);
            throw new FileException("Error during file read. " + e.getMessage());
        } finally {
            reader.close();
        }

        return stringBuilder.toString();
    }

}

