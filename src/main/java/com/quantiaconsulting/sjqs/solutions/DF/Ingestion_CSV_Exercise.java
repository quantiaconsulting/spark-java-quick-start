package com.quantiaconsulting.sjqs.solutions.DF;

import com.quantiaconsulting.sjqs.ML.BikeSharing;
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

import static org.apache.spark.sql.functions.*;

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

        Dataset<Row> tempDF = spark.read().option("sep","\t").option("header",true).csv(csvFile);

        tempDF.printSchema();

        tempDF.show();

        System.out.println("==================== WITH INFER SCHEMA ====================");

        tempDF = spark.read().option("sep","\t").option("header",true).option("inferSchema",true).csv(csvFile);

        tempDF.printSchema();

        tempDF.show();

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

        tempDF = spark.read().option("sep","\t").option("header",true).schema(schema).csv(csvFile);

        tempDF.printSchema();

        tempDF.show();

        Dataset<Row> tempDF2 = spark.read().option("sep","\t").option("header",true).schema(schema).csv(csvFile)
                .withColumnRenamed("n","n2")
                .withColumnRenamed("curr_title","curr_title2")
                .withColumnRenamed("prev_title","prev_title2")
                .withColumnRenamed("curr_id","curr_id2")
                .withColumnRenamed("prev_id","prev_id2")
                .withColumnRenamed("type","type2");

        Dataset<Row> joinDF = tempDF
                .join(tempDF2,tempDF.col("curr_id").equalTo(tempDF2.col("prev_id2")));

        joinDF.show();

        Dataset<Row> top10 = joinDF.withColumn("peso",expr("n + n2"))
                .orderBy(col("peso").desc())
                .select("prev_id","curr_id", "prev_id2", "peso","type")
      //          .where("prev_id is not null")
                .limit(10);

        top10.show();

        spark.stop();



    }
}
