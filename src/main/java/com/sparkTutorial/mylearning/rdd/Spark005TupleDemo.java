package com.sparkTutorial.mylearning.rdd;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class Spark005TupleDemo {

    public static void main(String[] args) {
        //find sqrt of 1st 1000 numbers
        /*
         * prepare a dataset like
         * 1 1.0
         * 2 1.4142135623730951
         * 3 1.7320508075688772
         * 4 2.0
         * 5 2.23606797749979
         * 6 2.449489742783178
         * 7 2.6457513110645907
         * 8 2.8284271247461903
         * 9 3.0
         *
         *
         * Tuples
         *
         *  val x1 = (4 ,2.0)
         *  val x2 = ("sam", "alice" ,"jack")
         *  val x3 = (object1 , object2 , object3)
         *
         *  Tuples help to store related value easyly
         */
        List<Integer> list = IntStream.rangeClosed(1, 1000).boxed().collect(Collectors.toList());
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        SparkConf conf = new SparkConf().setAppName("tuple05").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<Integer> originalnumbers = sc.parallelize(list);
        //JavaRDD<Double> squareRoot = originalnumbers.map(i -> Math.sqrt(i));
        JavaRDD<Tuple2<Integer, Double>> resultRdd = originalnumbers.map(i -> new Tuple2<Integer, Double>(1, Math.sqrt(i)));
        resultRdd.collect().forEach(System.out::println);
        sc.close();
    }
}
