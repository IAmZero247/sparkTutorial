package com.sparkTutorial.mylearning.rdd;

import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class Spark007PairRddReduceByKeyPreferedOverGroupByKey {
    //WARNING: Group By - Needs shuffling across nodes
    //Note - to get count always use reduce by key.
    public static void main(String[] args) {
        List<String> logSamples = new ArrayList<String>();
        logSamples.add( "WARN: Tuesday 4 September 0405");
        logSamples.add( "ERROR: Tuesday 4 September 0408");
        logSamples.add( "FATAL: Wednesday 5 September 1608");
        logSamples.add( "ERROR: Thurday 6 September 0405");
        logSamples.add( "WARN: Friday 7 September 1854");
        logSamples.add( "ERROR: Saturday 8 September 1942");
        //Get count of error.
        List<Integer> list = IntStream.rangeClosed(1, 1000).boxed().collect(Collectors.toList());
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        SparkConf conf = new SparkConf().setAppName("pairRdd07").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> originalLogMessages = sc.parallelize(logSamples);
        /* 1) groupBy
         * group by has performance issue - shuffling
         * size of iterator is obtained using guava iterables.size
         */
        JavaPairRDD<String, String> pairRddForGroup = originalLogMessages.mapToPair(i -> {
            String[] splits = i.split(":");
            return new Tuple2<String, String>(splits[0], splits[1]);
        });
        JavaPairRDD<String, Iterable<String>> pairRDDAfterGroupBy = pairRddForGroup.groupByKey();
        JavaRDD<Tuple2<String, Integer>> groupBy = pairRDDAfterGroupBy.map(i -> new Tuple2<String, Integer>(i._1, Iterables.size(i._2)));
        groupBy.foreach(i -> System.out.println(i._1 +" --> " +i._2));
        /* 1) reduceBy
         * prefered in cluster
         */
        JavaPairRDD<String, Long> pairRddForReduce = originalLogMessages.mapToPair(i -> new Tuple2<String, Long>(i.split(":")[0], 1l));
        JavaPairRDD<String, Long> reduceBy = pairRddForReduce.reduceByKey((val1, val2) -> val1 + val2);
        reduceBy.foreach(i -> System.out.println(i._1 +" --> " +i._2));
        sc.close();
    }

}
