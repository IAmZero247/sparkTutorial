package com.sparkTutorial.mylearning.dataframe;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

import java.util.Arrays;
import java.util.List;

import static org.apache.spark.sql.functions.*;

public class SparkDataFrame003PivotTableLoggingExample {
    //pivot table need 2 grouping and one aggregation
    public static void main(String[] args) {
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        SparkSession spark = SparkSession.builder().appName("sparkdf003").master("local[*]")
                .config("spark.sql.warehouse" , "file:///c:tmp" ) //only for windows
                .getOrCreate();
        Dataset<Row> dataframe = spark.read().option("header" ,"true").csv("in/narrow_and_wide_transformation/biglog.txt");
        dataframe.printSchema();

        Object[] months = new Object[] { "January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December", "NaMonth"};
        List<Object> columns = Arrays.asList(months);

        dataframe = dataframe.select(col("level"),
                date_format(col("datetime"), "MMMM").alias("month"),
                date_format(col("datetime"), "M").alias("monthnum").cast(DataTypes.IntegerType) );
        Dataset<Row> pivot1 =dataframe.groupBy(col("level")).pivot("monthnum").count();
        Dataset<Row> pivot2 = dataframe.groupBy("level").pivot("month", columns)
                .count()
                .na().fill(0); //Fill NA for zero/or not applicable
        pivot2.show(10);
        spark.close();



    }
}
