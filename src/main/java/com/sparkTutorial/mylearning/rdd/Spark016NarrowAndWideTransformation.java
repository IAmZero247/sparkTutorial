package com.sparkTutorial.mylearning.rdd;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

import java.util.Scanner;

public class Spark016NarrowAndWideTransformation {
    public static void main(String[] args) {
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        SparkConf conf = new SparkConf().setAppName("log-analysis").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> rdd= sc.textFile("in/narrow_and_wide_transformation/biglog.txt");
        //No of partitiond
        int partitions = rdd.getNumPartitions();
        System.out.println("no of partitions " + partitions);
        JavaPairRDD<String, String> narrowTransformation1 = rdd.mapToPair(line -> {
            String[] cols = line.split(",");
            return new Tuple2<>(cols[0], cols[1]);
        });
        JavaPairRDD<String, Iterable<String>> wideTransformation2 = narrowTransformation1.groupByKey();
        /*
         *Above shuffle is used twice (got from spark ui)
         * so need to cache or persist
         *
         * JavaPairRDD<String, Iterable<String>> cached = wideTransformation2.cache();
         * JavaPairRDD<String, Iterable<String>> persist = wideTransformation2.persist(StorageLevel.MEMORY_AND_DISK());
         *
         */
        JavaPairRDD<String, Iterable<String>> persist  = wideTransformation2.persist(StorageLevel.MEMORY_AND_DISK());

        persist.foreach(i -> System.out.println(i));

        Long count = persist.count();
        System.out.println("count " + count);
        Scanner scan = new Scanner(System.in);
        scan.nextLine();
        sc.close();


    }
}
