package com.sparkTutorial.mylearning.udf;


import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;

import java.text.DecimalFormat;

class UdfCelToFah  implements UDF1<Double, Double> {
    private static DecimalFormat df = new DecimalFormat("0.00");
    @Override
    public Double call(Double degreesCelcius) {
        String val = df.format(degreesCelcius * 9.0 / 5.0);
        return ( Double.valueOf(val)+ 32.0);
    }
}



public class SparkUdf001 {
    public static void main(String[] args) {
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        SparkSession spark = SparkSession.builder().appName("sparkudf001").master("local[*]")
                .config("spark.sql.warehouse" , "file:///c:tmp" ) //only for windows
                .getOrCreate();
        Dataset<Row> ds = spark.read().json("in/udf/temperatures.json");
        ds.createOrReplaceTempView("citytemps");
        spark.udf().register("CTOF", new UdfCelToFah(),  DataTypes.DoubleType);

        Dataset<Row> dsInFahrenheit = spark.sql("SELECT city, CTOF(avgLow) AS avgLowF, CTOF(avgHigh) AS avgHighF FROM citytemps");
        dsInFahrenheit.show();
        spark.close();

    }
}
