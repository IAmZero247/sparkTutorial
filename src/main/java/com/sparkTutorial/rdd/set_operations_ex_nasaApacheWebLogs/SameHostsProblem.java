package com.sparkTutorial.rdd.set_operations_ex_nasaApacheWebLogs;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class SameHostsProblem {

    public static void main(String[] args) throws Exception {

        /* "in/nasa_19950701.tsv" file contains 10000 log lines from one of NASA's apache server for July 1st, 1995.
           "in/nasa_19950801.tsv" file contains 10000 log lines for August 1st, 1995
           Create a Spark program to generate a new RDD which contains the hosts which are accessed on BOTH days.
           Save the resulting RDD to "out/nasa_logs_same_hosts.csv" file.

           Example output:
           vagrant.vf.mmc.com
           www-a1.proxy.aol.com
           .....

           Keep in mind, that the original log files contains the following header lines.
           host	logname	time	method	url	response	bytes

           Make sure the head lines are removed in the resulting RDD.
         */
        Logger.getLogger("org").setLevel(Level.ERROR);
        SparkConf conf = new SparkConf().setAppName("sameHosts").setMaster("local[1]");

        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> julyFirstLogs = sc.textFile("in/nasa_19950701.tsv");
        System.out.println("julyFirstLogs--> " + julyFirstLogs.count());
        JavaRDD<String> augustFirstLogs = sc.textFile("in/nasa_19950801.tsv");
        System.out.println("augustFirstLogs--> " + augustFirstLogs.count());
        JavaRDD<String> julyFirstHosts = julyFirstLogs.map(x-> x.split("\t")[0]);
        JavaRDD<String> augustFirstHosts = augustFirstLogs.map(x-> x.split("\t")[0]);
        JavaRDD<String> intersectionOfHosts =    julyFirstHosts.union(augustFirstHosts);
        System.out.println("augustFirstHosts--> " + augustFirstHosts.count());
        JavaRDD<String> cleanLogLines = augustFirstHosts.filter(SameHostsProblem::isNotHeader);
        System.out.println("cleanLogLines--> " + cleanLogLines.count());
        JavaRDD<String> sample = cleanLogLines.sample(true, 0.01);
        System.out.println("sample--> " + sample.count());
        sample.saveAsTextFile("out/nasa_logs_same_hosts.csv");
    }

    private static boolean isNotHeader(String line) {
        // header -> host	logname	time	method	url	response	bytes

        return !(line.trim().equalsIgnoreCase("host"));
    }
}
