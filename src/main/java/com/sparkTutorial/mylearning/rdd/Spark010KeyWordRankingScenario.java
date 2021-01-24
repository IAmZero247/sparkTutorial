package com.sparkTutorial.mylearning.rdd;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;

public class Spark010KeyWordRankingScenario  implements Serializable {
    private Set<String> borings = new HashSet<String>();

    public Spark010KeyWordRankingScenario() throws IOException {
        borings= Files.lines(Paths.get("in/subtitles/boringwords.txt")).collect(Collectors.toSet());

    }

    public static void main(String[] args) throws IOException {

        Logger.getLogger("org.apache").setLevel(Level.WARN);
        Spark010KeyWordRankingScenario keyWordRanking = new Spark010KeyWordRankingScenario();
        System.out.println("boring words count --> " + keyWordRanking.borings.size());
        SparkConf conf = new SparkConf().setAppName("keywordRanking10").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> initialRdd = sc.textFile("in/subtitles/input.txt");
        JavaRDD<String> sentencesWithLowercaseLettersOnlyRdd = initialRdd.map(line -> line.replaceAll("[^a-zA-Z\\s]", "").toLowerCase());
        JavaRDD<String> wordOnlyRdd = sentencesWithLowercaseLettersOnlyRdd.flatMap(x -> Arrays.asList(x.trim().split(" ")).iterator());
        JavaRDD<String> blankWordsRemoved = wordOnlyRdd.filter(word -> word.trim().length() > 0);
        JavaRDD<String> interestingWordOnly = blankWordsRemoved.filter(keyWordRanking::isNotBoring);
        //reduceAndCount
        JavaPairRDD<String, Long> pairRdd = interestingWordOnly.mapToPair(i -> new Tuple2<String, Long>(i, 1l));
        JavaPairRDD<String, Long> reduceByKey = pairRdd.reduceByKey((val1, val2) -> val1 + val2);
        //JavaPairRDD<Long, String> switched = reduceByKey.mapToPair(tuple -> new Tuple2<Long, String> (tuple._2, tuple._1 ));
        JavaPairRDD<Long, String> switched = reduceByKey.mapToPair(i -> i.swap());

        JavaPairRDD<Long, String> sorted =switched.sortByKey(false);
        System.out.println("No of partition Sorted -->" +  sorted.getNumPartitions() );
        /*
        //Do coalese if only u know the rdd will fit into one node. creates problem in cluster
        JavaPairRDD<Long, String>  coaleseTo1  = sorted.coalesce(1);
        System.out.println("No of partition coaleseTo1 -->" +  coaleseTo1.getNumPartitions() );
        System.out.println("Size of coaleseTo1 -->" +  coaleseTo1.count() );
         */
        List<Tuple2<Long, String>> take10 = sorted.take(10);
        take10.forEach(System.out::println);

        Scanner scan = new Scanner(System.in);
        scan.nextLine();
        sc.close();
    }

    public boolean isBoring(String word)
    {
        return borings.contains(word);
    }

    public boolean isNotBoring(String word)
    {
        return !isBoring(word);
    }

}
