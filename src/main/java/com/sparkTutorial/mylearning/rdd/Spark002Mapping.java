package com.sparkTutorial.mylearning.rdd;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class Spark002Mapping {

    public static void main(String[] args) {
        //find sqrt of 1st 1000 numbers
        List<Integer> list = IntStream.rangeClosed(1, 1000).boxed().collect(Collectors.toList());
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        SparkConf conf = new SparkConf().setAppName("mapping02").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<Integer> rddList = sc.parallelize(list);
        JavaRDD<Double> sqrtRdd = rddList.map(i -> Math.sqrt(i));
        sc.close();
    }
}