package com.sparkTutorial.mylearning;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

public class Starter {

    public static void main(String[] args) {
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        SparkConf conf = new SparkConf().setAppName("wordCounts").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.close();
    }


}
