package com.sparkTutorial.rdd.sumOfNumbers;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.stream.IntStream;

public class SumOfNumbersProblem {

    public static void main(String[] args) throws Exception {

        /* Create a Spark program to read the first 100 prime numbers from in/prime_nums.text,
           print the sum of those numbers to console.

           Each row of the input file contains 10 prime numbers separated by spaces.
         */
        Logger.getLogger("org").setLevel(Level.ERROR);
        SparkConf conf = new SparkConf().setAppName("sameHosts").setMaster("local[1]");
        JavaSparkContext sc =  new JavaSparkContext(conf);
        JavaRDD<String> lines = sc.textFile("in/prime_nums.text");
        JavaRDD<Integer> numbers = lines.flatMap(l-> {
            String[] splits = l.split("\\s+");
            return Arrays.asList( splits).iterator();
        }).filter(i -> !i.trim().equalsIgnoreCase("")).map(Integer::valueOf);
        System.out.println("numbers -->" + numbers.count());
        JavaRDD<Integer> filteredPrime = numbers.filter(SumOfNumbersProblem::isPrime);
        System.out.println("filteredPrime -->" + filteredPrime.count());
    }

    public static boolean isPrime(Integer value){
       boolean check = IntStream.of(2,(int)value/2).filter(i-> i!=2 && i%2==0).anyMatch(i ->value/i==0);
       System.out.println(check);
       return check;
    }
}
