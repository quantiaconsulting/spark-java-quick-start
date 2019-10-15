package com.quantiaconsulting.sjqs.df.codeBrowsing;

import com.quantiaconsulting.sjqs.ml.codeBrowsing.BikeSharing;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;

import static org.apache.spark.sql.functions.col;

public class Preparation_DF_Basics {
    public static void main(String[] args) {

        String path = BikeSharing.class.getProtectionDomain().getCodeSource().getLocation().getPath();
        String decodedPath = null;
        try {
            decodedPath = URLDecoder.decode(path.substring(0, path.lastIndexOf("/")), "UTF-8");
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        String parquetFile = decodedPath + "/resources/wikipedia_pageviews_by_second.parquet";

        SparkSession spark = SparkSession
                .builder()
                .appName("Simple Application")
                .getOrCreate();

        Dataset<Row> tempDF = spark
                .read()
                .parquet(parquetFile);

        tempDF.cache();
        tempDF.printSchema();

        //System.out.println("===============================\n il partquet ha "+tempDF.count()+" linee");

        //DF API: Create a DF :
        //    - select timestamp and requests
        //    - filter out rows with less than 1000 requests
        //    - order by requests (desc)

        Dataset<Row> partialres = tempDF.where("requests >= 1000");
        Dataset<Row> partialres2 = partialres.select("timestamp","requests");
        Dataset<Row> partialres3 = partialres2.orderBy(col("requests").desc_nulls_last());
        //System.out.println("===============================\n il partquet ha "+partialres2.count()+" linee con requests >= 1000 ");
        partialres2.show();
        partialres3.show();

        partialres3.explain();

        //Temp Table and sql: Perform the same query in SQL using a temp view

        tempDF.createOrReplaceTempView("richieste");
        Dataset<Row> sqlRes = spark.sql("SELECT timestamp, requests FROM richieste WHERE requests >= 1000 ORDER BY requests DESC");

        sqlRes.explain();

        sqlRes.show();

        spark.stop();
    }
}