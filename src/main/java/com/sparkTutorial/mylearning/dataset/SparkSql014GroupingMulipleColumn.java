package com.sparkTutorial.mylearning.dataset;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

public class SparkSql014GroupingMulipleColumn {
    public static void main(String[] args) {
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        SparkSession spark = SparkSession.builder().appName("sparksql014").master("local[*]")
                .config("spark.sql.warehouse" , "file:///c:tmp" ) //only for windows
                .getOrCreate();
        /*
        List<Row> inMemory = new ArrayList<Row>();
        inMemory.add(RowFactory.create("WARN", "2016-12-31 04:19:32"));
        inMemory.add(RowFactory.create("FATAL", "2016-12-31 03:22:34"));
        inMemory.add(RowFactory.create("WARN", "2016-12-31 03:21:21"));
        inMemory.add(RowFactory.create("INFO", "2015-4-21 14:32:21"));
        inMemory.add(RowFactory.create("FATAL","2015-4-21 19:23:20"));

        StructField[] fields = new StructField[] {
                new StructField("level", DataTypes.StringType, false, Metadata.empty()),
                new StructField("datetime", DataTypes.StringType, false, Metadata.empty())
        };

        StructType schema = new StructType(fields);
        Dataset<Row> dataset = spark.createDataFrame(inMemory, schema);
         */
        Dataset<Row> dataset = spark.read().option("header" ,"true").csv("in/narrow_and_wide_transformation/biglog.txt");

        dataset.createOrReplaceTempView("logging_table");


        /*adding month to temp view
         *Dataset<Row> datasetWithMonth = spark.sql("select level, date_format(datetime, 'MMMM') AS month from logging_table");
         *datasetWithMonth.createOrReplaceTempView("logging_table");
         *Dataset<Row> results = spark.sql("select level, month, count(1) as total from logging_table group by level, month");
         *results.show();
         */

        Dataset<Row> result = spark.sql("select level, date_format(datetime, 'MMMM') AS month, count(1) as total from logging_table group by level, month ");
        result.show();

        spark.close();
    }
}
