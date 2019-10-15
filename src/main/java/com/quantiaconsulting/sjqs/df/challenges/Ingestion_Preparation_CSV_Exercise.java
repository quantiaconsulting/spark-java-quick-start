package com.quantiaconsulting.sjqs.df.challenges;

import com.quantiaconsulting.sjqs.ml.codeBrowsing.BikeSharing;
import org.apache.spark.sql.SparkSession;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;

public class Ingestion_Preparation_CSV_Exercise {
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

        /*
        data la tabella 2015_02_clickstream.tsv che rappresenta il click stream di wikipedia nel mese di febbraio del 2015
        cercare i 10 percorsi di lunghezza 2 più gettonati
        HINTs:
        - partire dall'esercizio Ingestion_CSV_Exercise che abbiamo fatto ieri
        - rinominare i campi di una delle due tabelle in modo da non avere gli stessi nomi per le colonne delle due tabelle
        - mettere in join la tabella con la sua copia rinominta in modo che il curr_id della tabella originale sia uguale al prev_id della copia rinominata
        - calcolare la somma del numero di click sui due percorsi (il valori della colonna n nella tabella originale + il valore di n nella copia rinominata)
        - ordinare in modo discendente per tale somma

            Schema:
            - prev_id -> int
            - curr_id -> int
            - n -> int
            - prev_title -> string
            - curr_title -> string
            - type -> string
        Read the csv by manually create the schema

         */

//        List<StructField> fields = new ArrayList<>();
//
//        StructField field = <FILL>
//
//        Dataset<Row> tempDF = spark.read().<FILL>
//        tempDF.printSchema();
//        tempDF.show();
//
//        Dataset<Row> tempDF2 = spark.read().<FILL>.csv(csvFile)
//                .withColumnRenamed("n","n2")
//                <FILL>
//
//        Dataset<Row> joinDF = tempDF
//                .join<FILL>
//        joinDF.show();
//
//        Dataset<Row> top10 = <FILL>
        spark.stop();
    }
}
