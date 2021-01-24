package com.sparkTutorial.mylearning.dataset;

import com.sparkTutorial.mylearning.dataset.javabeans.NumberEngToFre;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import scala.Tuple3;

import java.util.Arrays;
import java.util.List;

import static org.apache.spark.sql.functions.col;

public class SparkSql003ComplexTypeJavaBean {
    public static void main(String[] args) {
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        SparkSession spark = SparkSession.builder().appName("sparksql003").master("local[*]")
                .config("spark.sql.warehouse" , "file:///c:tmp" ) //only for windows
                .getOrCreate();
        Encoder<NumberEngToFre> numberEncoder = Encoders.bean(NumberEngToFre.class);
        //
        // Create a container of the JavaBean instances
        //
        List<NumberEngToFre> data = Arrays.asList(
                new NumberEngToFre(1, "one", "un"),
                new NumberEngToFre(2, "two", "deux"),
                new NumberEngToFre(3, "three", "trois"));

        Dataset<NumberEngToFre> ds = spark.createDataset(data, numberEncoder);
        System.out.println("*** here is the schema inferred from the bean");
        ds.printSchema();

        // Use the convenient bean-inferred column names to query
        ds.where(col("i").gt(2)).select(col("english"), col("french")).show();
        spark.close();

    }
}
