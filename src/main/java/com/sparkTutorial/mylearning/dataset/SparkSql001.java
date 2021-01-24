package com.sparkTutorial.mylearning.dataset;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;
import java.util.List;

public class SparkSql001 {
    public static void main(String[] args) {
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        SparkSession spark = SparkSession.builder().appName("sparksql001").master("local[*]")
                .config("spark.sql.warehouse" , "file:///c:tmp" ) //only for windows
                .getOrCreate();
        List<Integer> data = Arrays.asList(10, 11, 12, 13, 14, 15);
        Dataset<Integer> ds = spark.createDataset(data, Encoders.INT());
        System.out.println("*** only one column, and it always has the same name");
        ds.printSchema();
        ds.show(3);

        System.out.println("*** values > 12");
        // the harder way to filter
        Dataset<Integer> ds2 = ds.filter((Integer value) -> value > 12);

        ds2.show();

        Long ds2Size = ds2.count();
        System.out.println("ds2Size --> " +ds2Size);
        spark.close();
    }
}
