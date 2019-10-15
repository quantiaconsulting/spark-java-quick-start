package com.quantiaconsulting.sjqs.df.codeBrowsing;

import com.quantiaconsulting.sjqs.solutions.ml.BikeSharing;
import org.apache.spark.sql.AnalysisException;
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


public class Ingestion_CSV_WithSchema {
    public static void main(String[] args) throws AnalysisException {

        String path = BikeSharing.class.getProtectionDomain().getCodeSource().getLocation().getPath();
        String decodedPath = null;
        try {
            decodedPath = URLDecoder.decode(path.substring(0, path.lastIndexOf("/")), "UTF-8");
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        String csvFile = decodedPath + "/resources/wikipedia_pageviews_by_second.csv";

        SparkSession spark = SparkSession
                .builder()
                .appName("Simple Application")
                .getOrCreate();

        List<StructField> fields = new ArrayList<>();

        StructField field = DataTypes.createStructField("timestamp", DataTypes.TimestampType, true);
        fields.add(field);

        field = DataTypes.createStructField("site", DataTypes.StringType, true);
        fields.add(field);

        field = DataTypes.createStructField("requests", DataTypes.IntegerType, true);
        fields.add(field);

        StructType schema = DataTypes.createStructType(fields);

        Dataset<Row> tempDF = spark
                .read()
                .option("sep", ",")
                .option("header", "true")
                .schema(schema)
                .csv(csvFile);

        tempDF.cache();
        tempDF.printSchema();
        tempDF.show(10);
        System.out.println(tempDF.count());

        spark.stop();
    }
}
