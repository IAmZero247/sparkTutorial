package com.sparkTutorial.mylearning.rdd;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class Spark004CountOperationUsingReduce {

    public static void main(String[] args) {
        List<String> courses = Arrays.asList(new String[]{"Spring", "Spring Boot", "API" , "Microservices","AWS", "PCF","Azure", "Docker", "Kubernetes"});
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        SparkConf conf = new SparkConf().setAppName("count04").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> rdd = sc.parallelize(courses);
        JavaRDD<Long> mappedTo1 = rdd.map(i ->1l );
        Long count = mappedTo1.reduce((i1,i2) -> i1+i2);
        System.out.println(count);
        sc.close();


    }
}
