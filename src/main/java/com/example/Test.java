package com.example;

import tech.allegro.schema.json2avro.converter.AvroConversionException;
import tech.allegro.schema.json2avro.converter.JsonAvroConverter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.Schema;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

public class Test {
    public static void main(String[] args) throws IOException {
        // Avro schema with one string field: username
        String schema = Files.readString(Paths.get("schema/Departments.avsc"));
        String json = Files.readString(Paths.get("data/departments.json"));

        JsonAvroConverter converter = new JsonAvroConverter();

        // conversion to binary Avro
        //byte[] avro = converter.convertToAvro(json.getBytes(), schema);

        // conversion to GenericData.Record
        //GenericData.Record record = converter.convertToGenericDataRecord(json.getBytes(), new Schema.Parser().parse(schema));

        // conversion from binary Avro to JSON
        //byte[] binaryJson = converter.convertToJson(avro, schema);

        // exception handling
        //String invalidJson = "{ \"username\": 8 }";

        /*try {
            converter.convertToAvro(invalidJson.getBytes(), schema);
        } catch (AvroConversionException ex) {
            System.err.println("Caught exception: " + ex.getMessage());
        }**/
    }
}
