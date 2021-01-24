package com.sparkTutorial.rdd.map_ex_airports;

import com.sparkTutorial.rdd.commons.Utils;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class AirportsInUsaProblem {

    public static void main(String[] args) throws Exception {

        /*
           SampleData -> *Data -> 200,"Port Hardy","Port Hardy","Canada","YZT","CYZT",50.680556,-127.366667,71,-8,"A","America/Vancouver"
             * AirportId 200
             * Name Port Hardy
             * City Port Hardy
             * Country Canada
             * IATA/FAA_Code YZT
             * ICAO_Code CYZT
             * Latitude 50.680556
             * Longitude -127.366667
             * Altitude 71
             * Timezone -8
             * DST A
             * TimezoneInOlison America/Vancouver

           Create a Spark program to read the airport data from in/airports.text, find all the airports which are located in United States
           and output the airport's name and the city's name to out/airports_in_usa.text.

           Each row of the input file contains the following columns:
           Airport ID, Name of airport, Main city served by airport, Country where airport is located, IATA/FAA code,
           ICAO Code, Latitude, Longitude, Altitude, Timezone, DST, Timezone in Olson format

           Sample output:
           "Putnam County Airport", "Greencastle"
           "Dowagiac Municipal Airport", "Dowagiac"
           ...
         */
        Logger.getLogger("org").setLevel(Level.ERROR);
        SparkConf conf = new SparkConf().setAppName("airport").setMaster("local[3]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> airports = sc.textFile("in/airports.text");
        JavaRDD<String> airportsInUsa = airports.filter(i-> i.split(Utils.COMMA_DELIMITER)[3].equals("\"United States\""));
        JavaRDD<String> airportNameAndCityNames =   airportsInUsa.map(e -> {String[] splits = e.split(Utils.COMMA_DELIMITER);
               return   StringUtils.join(new String[] {splits[1], splits[2]} , ",");
            });
        airportNameAndCityNames.saveAsTextFile("out/airport_in_usa.text");
    }
}
