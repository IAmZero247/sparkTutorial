package com.sparkTutorial.mylearning.udf;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import java.text.SimpleDateFormat;
import java.util.Date;

public class SparkUdf003OnSparkSql {
    //Refer - >SparkSql015Ordering as starting point
    public static void main(String[] args) {
        //replace the calculation with udf
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        SparkSession spark = SparkSession.builder().appName("sparkudf003").master("local[*]")
                .config("spark.sql.warehouse" , "file:///c:tmp" ) //only for windows
                .getOrCreate();

        //udf1 starts
        SimpleDateFormat input1 = new SimpleDateFormat("YYYY-MM-DD HH:mm:ss");
        SimpleDateFormat input2 = new SimpleDateFormat("MMM");
        SimpleDateFormat output = new SimpleDateFormat("M");
        spark.udf().register("monthNumFromDatetimeString", (String datetime) -> {
            Date date = input1.parse(datetime);
            return Integer.parseInt(output.format(date));
        }, DataTypes.IntegerType);
        //udf1 ends
        //udf2 starts
        spark.udf().register("monthNumFromMonthString", (String month) -> {
            Date date = input2.parse(month);
            return Integer.parseInt(output.format(date));
        }, DataTypes.IntegerType);
        //udf2 ends
        Dataset<Row> dataset = spark.read().option("header" ,"true").csv("in/narrow_and_wide_transformation/biglog.txt");
        dataset.createOrReplaceTempView("logging_table");
        dataset.printSchema();

        //udf1 sample
        /*
         *Dataset<Row> result = spark.sql("select level, date_format(datetime, 'MMMM') AS month,  monthNumFromDatetimeString(datetime) AS monthInNum, count(1) AS total  from logging_table group by level, month, monthInNum order by monthInNum, level");
         *result=result.drop("monthInNum");
         */
        Dataset<Row> result = spark.sql("select level, date_format(datetime, 'MMMM') AS month, count(1) as total from logging_table group by level, month order by monthNumFromMonthString(month), level");

        result.show(100);
        spark.close();
    }
}
