package com.sparkTutorial.rdd.set_operations_ex_nasaApacheWebLogs;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class UnionLogProblem {
    //https://www.level-up.one/sampling-with-replacement-sampling-without-replacement/

    public static void main(String[] args) throws Exception {

        /* "in/nasa_19950701.tsv" file contains 10000 log lines from one of NASA's apache server for July 1st, 1995.
           "in/nasa_19950801.tsv" file contains 10000 log lines for August 1st, 1995
           Create a Spark program to generate a new RDD which contains the log lines from both July 1st and August 1st,
           take a 0.1 sample of those log lines and save it to "out/sample_nasa_logs.tsv" file.

           Keep in mind, that the original log files contains the following header lines.
           host	logname	time	method	url	response	bytes

           Make sure the head lines are removed in the resulting RDD.
         */
        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("org").setLevel(Level.ERROR);
        SparkConf conf = new SparkConf().setAppName("sameHosts").setMaster("local[1]");

        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> julyFirstLogs = sc.textFile("in/nasa_19950701.tsv");
        System.out.println("julyFirstLogs--> " + julyFirstLogs.count());
        JavaRDD<String> augustFirstLogs = sc.textFile("in/nasa_19950801.tsv");
        System.out.println("augustFirstLogs--> " + augustFirstLogs.count());
        JavaRDD<String> aggregatedLogLines =    julyFirstLogs.union(augustFirstLogs);
        System.out.println("aggregatedLogLines--> " + aggregatedLogLines.count());
        JavaRDD<String> cleanLogLines = aggregatedLogLines.filter(UnionLogProblem::isNotHeader);
        System.out.println("cleanLogLines--> " + cleanLogLines.count());
        JavaRDD<String> sample = cleanLogLines.sample(true, 0.1);
        System.out.println("sample--> " + sample.count());
        sample.saveAsTextFile("out/sample_nasa_logs.csv");
    }
    private static boolean isNotHeader(String line) {
        // header -> host	logname	time	method	url	response	bytes

        return !(line.startsWith("host") && line.contains("logname")
                && line.contains("time") && line.contains("method")
                && line.contains("url") && line.contains("response")
                && line.contains("bytes"));
    }
}
