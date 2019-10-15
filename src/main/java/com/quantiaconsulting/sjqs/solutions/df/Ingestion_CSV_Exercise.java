package com.quantiaconsulting.sjqs.solutions.df;

import com.quantiaconsulting.sjqs.ml.codeBrowsing.BikeSharing;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.List;

public class Ingestion_CSV_Exercise {
    public static void main(String[] args) {
        String path = BikeSharing.class.getProtectionDomain().getCodeSource().getLocation().getPath();
        String decodedPath = null;
        try {
            decodedPath = URLDecoder.decode(path.substring(0, path.lastIndexOf("/")), "UTF-8");
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        String csvFile = decodedPath + "/resources/2015_02_clickstream.tsv";

        SparkSession spark = SparkSession
                .builder()
                .appName("Simple Application")
                .getOrCreate();

        System.out.println("==================== WITHOUT INFER SCHEMA ====================");

        Dataset<Row> tempDF1 = spark.read().option("sep","\t").option("header",true).csv(csvFile);
        tempDF1.printSchema();
        tempDF1.show();

        System.out.println("==================== WITH INFER SCHEMA ====================");

        Dataset<Row> tempDF2 = spark.read().option("sep","\t").option("header",true).option("inferSchema",true).csv(csvFile);
        tempDF2.printSchema();
        tempDF2.show();

        System.out.println("==================== WITH SCHEMA ====================");

        List<StructField> fields = new ArrayList<>();

        StructField field = DataTypes.createStructField("prev_id", DataTypes.IntegerType, true);
        fields.add(field);

        field = DataTypes.createStructField("curr_id", DataTypes.IntegerType, true);
        fields.add(field);

        field = DataTypes.createStructField("n", DataTypes.IntegerType, true);
        fields.add(field);

        field = DataTypes.createStructField("prev_title", DataTypes.StringType, true);
        fields.add(field);

        field = DataTypes.createStructField("curr_title", DataTypes.StringType, true);
        fields.add(field);

        field = DataTypes.createStructField("type", DataTypes.StringType, true);
        fields.add(field);

        StructType schema = DataTypes.createStructType(fields);

        Dataset<Row> tempDF3 = spark.read().option("sep","\t").option("header",true).schema(schema).csv(csvFile);
        tempDF3.printSchema();
        tempDF3.show();

        spark.stop();
    }
}
