package com.sparkTutorial.rdd.map_ex_airports;

import com.sparkTutorial.rdd.commons.Utils;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class AirportsByLatitudeProblem {

    public static void main(String[] args) throws Exception {

        /* Create a Spark program to read the airport data from in/airports.text,  find all the airports whose latitude are bigger than 40.
           Then output the airport's name and the airport's latitude to out/airports_by_latitude.text.

           Each row of the input file contains the following columns:
           Airport ID, Name of airport, Main city served by airport, Country where airport is located, IATA/FAA code,
           ICAO Code, Latitude, Longitude, Altitude, Timezone, DST, Timezone in Olson format

           Sample output:
           "St Anthony", 51.391944
           "Tofino", 49.082222
           ...
         */
        Logger.getLogger("org").setLevel(Level.ERROR);
        SparkConf conf = new SparkConf().setAppName("airport").setMaster("local[3]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> airports = sc.textFile("in/airports.text");
        JavaRDD<String> airportsInUsa = airports.filter(i->
                Double.valueOf(i.split(Utils.COMMA_DELIMITER)[6]) > 40d);
        JavaRDD<String> airportsNameAndLatitude = airports.map(i-> {

                    String[] splits = i.split(Utils.COMMA_DELIMITER);
            return StringUtils.join(new String [] {splits[1], splits[6]}, ",");
        });
        airportsNameAndLatitude.saveAsTextFile("out/airports_by_latitude.text");

    }
}
