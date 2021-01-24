package com.sparkTutorial.rdd.flatmap_ex_wordcount;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.Map;

public class WordCount {
    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.ERROR);
        SparkConf conf = new SparkConf().setAppName("wordCounts").setMaster("local[3]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> lines = sc.textFile("in/word_count.text");
        System.out.println("lines -->" + lines.count());
        //flat map always return iterator
        JavaRDD<String> flattend = lines.flatMap( x -> Arrays.asList(x.split(" ")).iterator());
        System.out.println("flattend -->" + flattend.count());
        Map<String, Long> countByValue = flattend.countByValue();
        System.out.println("countByValue -->" + countByValue.size());
        for(Map.Entry<String, Long> entry : countByValue.entrySet()){
            System.out.println(entry.getKey()+"->"+entry.getValue());
        }

    }
}
