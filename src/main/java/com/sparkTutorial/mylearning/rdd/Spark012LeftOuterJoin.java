package com.sparkTutorial.mylearning.rdd;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

public class Spark012LeftOuterJoin {
    public static void main(String[] args) {
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        SparkConf conf = new SparkConf().setAppName("leftOuterJoin12").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        //id visit
        List<Tuple2<Integer, Integer>> visits = new ArrayList<>();
        visits.add(new Tuple2<>(4 ,18));
        visits.add(new Tuple2<>(6 ,4));
        visits.add(new Tuple2<>(10 ,9));
        //id name
        List<Tuple2<Integer, String>> users = new ArrayList<>();
        users.add(new Tuple2<>(1 ,"John"));
        users.add(new Tuple2<>(2 ,"Alice"));
        users.add(new Tuple2<>(3 ,"Mary"));
        users.add(new Tuple2<>(4 ,"Jack"));
        users.add(new Tuple2<>(5 ,"Stefen"));
        users.add(new Tuple2<>(6 ,"Kate"));
        JavaPairRDD<Integer, Integer> visitRdd = sc.parallelizePairs(visits);
        JavaPairRDD<Integer, String>  userRdd = sc.parallelizePairs(users);
        //JavaPairRDD<Integer, Tuple2<String, Integer>>
        JavaPairRDD<Integer, Tuple2<String, Optional<Integer>>> leftJoin = userRdd.leftOuterJoin(visitRdd);
        //Print all visits
        List<Tuple2<Integer, Tuple2<String, Optional<Integer>>>> collect = leftJoin.collect();
        collect.forEach(i -> System.out.println( i._1 +" --> " + i._2._1 + " --> " + i._2._2.orElse(0)));
        sc.close();
    }
}
