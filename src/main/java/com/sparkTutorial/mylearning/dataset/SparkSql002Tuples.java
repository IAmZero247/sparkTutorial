package com.sparkTutorial.mylearning.dataset;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import scala.Tuple3;

import java.util.Arrays;
import java.util.List;

import static org.apache.spark.sql.functions.col;

public class SparkSql002Tuples {
    public static void main(String[] args) {
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        SparkSession spark = SparkSession.builder().appName("sparksql002").master("local[*]")
                .config("spark.sql.warehouse" , "file:///c:tmp" ) //only for windows
                .getOrCreate();
        List<Tuple3<Integer, String, String>> tuples =
                Arrays.asList(
                        new Tuple3<>(1, "one", "un"),
                        new Tuple3<>(2, "two", "deux"),
                        new Tuple3<>(3, "three", "trois"));
        Encoder<Tuple3<Integer, String, String>> encoder =
                Encoders.tuple(Encoders.INT(), Encoders.STRING(), Encoders.STRING());
        Dataset<Tuple3<Integer, String, String>> tupleDS = spark.createDataset(tuples, encoder);
        System.out.println("*** Tuple Dataset types");
        tupleDS.printSchema();


        //
        // Create a Spark Dataset from an array of tuples. The inferred schema doesn't
        // have convenient column names but it can still be queried conveniently.
        //
        System.out.println("*** filter by one column and fetch another");
        tupleDS.where(col("_1").gt(2)).select(col("_2"), col("_3")).show();

        spark.close();

    }
}
