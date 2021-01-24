package com.sparkTutorial.mylearning.udf;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.types.DataTypes;

import java.util.Arrays;
import java.util.List;
import static org.apache.spark.sql.functions.*;

public class SparkUdf002AddColumnValUsingUdf {
    public static void main(String[] args) {
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        SparkSession spark = SparkSession.builder().appName("sparkudf002").master("local[*]")
                .config("spark.sql.warehouse" , "file:///c:tmp" ) //only for windows
                .getOrCreate();
        Dataset<Row> dataframe = spark.read().option("header" ,"true").csv("in/exams/students.csv");
        dataframe.printSchema();

        /*
        udf column pass
        For Maths - if grade above B is pass
        For Rest  - gradle above C is pass
         */
        UDF2 evaluatePass = new UDF2<String, String,String>(){
            List<String> mathPass = Arrays.asList(new String[]{"A", "A+", "B", "B+"});
            List<String> generalPass = Arrays.asList(new String[]{"A", "A+", "B", "B+", "C", "C+"});
            @Override
            public String call(String subject, String grade) throws Exception {
                if(subject.toLowerCase().equals("math")){
                  return mathPass.contains(grade.toUpperCase())? "Pass":"Fail";
                }
                return generalPass.contains(grade.toUpperCase())? "Pass":"Fail";
            }
        };
        spark.udf().register("hasPass", evaluatePass, DataTypes.StringType);
        dataframe = dataframe.withColumn("EvaluatePass", callUDF("hasPass", col("subject"), col("grade")));
        dataframe.show(100);
        spark.close();

    }
}
