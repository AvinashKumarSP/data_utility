package com.example;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;

import java.io.*;
import java.net.URISyntaxException;
import java.net.URL;

public class Converter {
    public static void main(String[] args) throws IOException, URISyntaxException {
        DatumReader<GenericRecord> datumReader = new GenericDatumReader<>();
        Converter converter = new Converter();
        File avroFile = converter.getFileFromResource("data/departments.avro");
        File targetFile = new File(args[0]);
        String fileName = "";
        DataFileReader<GenericRecord> dataFileReader = new DataFileReader<>(
                avroFile, datumReader);
        Schema schema = dataFileReader.getSchema();
        System.out.println(schema);
        converter.writeFileToLocalFS(targetFile, dataFileReader);
    }

    private File getFileFromResource(String fileName) throws URISyntaxException {

        ClassLoader classLoader = getClass().getClassLoader();
        URL resource = classLoader.getResource(fileName);
        if (resource == null) {
            throw new IllegalArgumentException("file not found! " + fileName);
        } else {

            // failed if files have whitespaces or special characters
            //return new File(resource.getFile());

            return new File(resource.toURI());
        }

    }

    /*private File getFileFromLocalFS(File fileName) throws URISyntaxException, IOException {
        LineIterator it = FileUtils.lineIterator(fileName, "UTF-8");
        try {
            while (it.hasNext()) {
                String line = it.nextLine();
                // do something with line
            }
        } finally {
            LineIterator.closeQuietly(it);
        }
        ClassLoader classLoader = getClass().getClassLoader();
        URL resource = classLoader.getResource(fileName);
        if (resource == null) {
            throw new IllegalArgumentException("file not found! " + fileName);
        } else {

            // failed if files have whitespaces or special characters
            //return new File(resource.getFile());

            return new File(resource.toURI());
        }

    }**/

    private File getFileS3(String fileName) throws URISyntaxException {

        ClassLoader classLoader = getClass().getClassLoader();
        URL resource = classLoader.getResource(fileName);
        if (resource == null) {
            throw new IllegalArgumentException("file not found! " + fileName);
        } else {

            // failed if files have whitespaces or special characters
            //return new File(resource.getFile());

            return new File(resource.toURI());
        }

    }

    private void writeFileToLocalFS(File targetFile,
                                    DataFileReader<GenericRecord> dataFileReader)
            throws IOException {

        FileWriter fileWriter = new FileWriter(targetFile, true);
        BufferedWriter writer = new BufferedWriter(fileWriter);
        PrintWriter printWriter = new PrintWriter(writer);
        GenericRecord record = null;
        while (dataFileReader.hasNext()) {
            record = dataFileReader.next(record);
            String jsonString = record.toString();
            System.out.println(jsonString);
            printWriter.println(jsonString);
        }
        printWriter.close();
    }



}
