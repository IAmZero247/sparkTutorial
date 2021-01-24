package com.sparkTutorial.mylearning.rdd;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.math.BigInteger;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class Spark001Reduce {
    public static void main(String[] args) {

        //1)Create a List  1-100000 and sum-up
        //2)Find factorial of 100000

        //1)
        List<Integer> list = IntStream.rangeClosed(1, 100000).boxed().collect(Collectors.toList());
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        SparkConf conf = new SparkConf().setAppName("reduce01").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<Integer> rddList = sc.parallelize(list);
        Integer sum  = rddList.reduce((x,y) -> x+y );
        System.out.println("sum --> "+sum);
        //2)
        JavaRDD<BigInteger> rddMappedToBigInteger = rddList.map(x-> (BigInteger.valueOf(x)));
        BigInteger factorial  = rddMappedToBigInteger.reduce((x,y) -> x.multiply(y));
        System.out.println("factorial --> "+factorial);
        sc.close();

    }
}

