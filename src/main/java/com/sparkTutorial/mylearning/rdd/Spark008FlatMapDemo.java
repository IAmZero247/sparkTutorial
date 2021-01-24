package com.sparkTutorial.mylearning.rdd;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class Spark008FlatMapDemo {

    public static void main(String[] args) {
        List<String> logSamples = new ArrayList<String>();
        logSamples.add( "WARN Tuesday 4 September 0405");
        logSamples.add( "ERROR Tuesday 4 September 0408");
        logSamples.add( "FATAL Wednesday 5 September 1608");
        logSamples.add( "ERROR Thurday 6 September 0405");
        logSamples.add( "WARN Friday 7 September 1854");
        logSamples.add( "ERROR Saturday 8 September 1942");

        /*
         *word count
         *note ->  flatmap always need an iterable as return
         */

        List<Integer> list = IntStream.rangeClosed(1, 1000).boxed().collect(Collectors.toList());
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        SparkConf conf = new SparkConf().setAppName("flatmap08").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> rdd = sc.parallelize(logSamples);
        JavaRDD<String> flattened = rdd.flatMap(l -> Arrays.asList(l.split(" ")).iterator());
        //flattened.foreach(System.out::println);
        flattened.collect().forEach(System.out::println);
        sc.close();
    }
}
