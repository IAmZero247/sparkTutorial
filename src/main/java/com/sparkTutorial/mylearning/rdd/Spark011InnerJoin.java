package com.sparkTutorial.mylearning.rdd;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

public class Spark011InnerJoin {

    public static void main(String[] args) {
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        SparkConf conf = new SparkConf().setAppName("innerJoin11").setMaster("local[*]");
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
        JavaPairRDD<Integer, Tuple2<String, Integer>> joinUserToVisit = userRdd.join(visitRdd);
        JavaPairRDD<Integer, Tuple2<Integer, String>> joinVisitToUser = visitRdd.join(userRdd);
        //printing

        joinUserToVisit.collect().forEach(System.out::println);
        joinVisitToUser.collect().forEach(System.out::println);
        sc.close();
    }
}
