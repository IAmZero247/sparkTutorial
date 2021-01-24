package com.sparkTutorial.mylearning.dataset;

import com.sparkTutorial.mylearning.dataset.javabeans.NamedPoints;
import com.sparkTutorial.mylearning.dataset.javabeans.Point;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import static org.apache.spark.sql.functions.*;

public class SparkSql006Map {
    public static void main(String[] args) {
        Encoder<NamedPoints> namedPointsEncoder = Encoders.bean(NamedPoints.class);
        HashMap<String, Point> points1 = new HashMap<>();
        points1.put("p1", new Point(0.0, 0.0));
        HashMap<String, Point> points2 = new HashMap<>();
        points2.put("p1", new Point(0.0, 0.0));
        points2.put("p2", new Point(2.0, 6.0));
        points2.put("p3", new Point(10.0, 100.0));
        List<NamedPoints> namedPoints = Arrays.asList(
                new NamedPoints("a", points1),
                new NamedPoints("b", points2)
        );
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        SparkSession spark = SparkSession.builder().appName("sparksql006").master("local[*]")
                .config("spark.sql.warehouse" , "file:///c:tmp" ) //only for windows
                .getOrCreate();
        System.out.println("*** Example 1: maps");

        //
        // In Spark 2.0 this throws
        // java.lang.UnsupportedOperationException: map type is not supported currently
        // See https://issues.apache.org/jira/browse/SPARK-16706 -- and
        // notice it has been marked Fixed for Spark 2.1.0.
        //
        Dataset<NamedPoints> namedPointsDS =
                spark.createDataset(namedPoints, namedPointsEncoder);

        System.out.println("*** here is the schema inferred from the bean");
        namedPointsDS.printSchema();

        System.out.println("*** here is the data");
        namedPointsDS.show();

        System.out.println("*** filter and select using map lookup");
        namedPointsDS
                .where(size(col("points")).gt(1))
                .select(col("name"),
                        size(col("points")).as("count"),
                        col("points").getItem("p1")).show();
        spark.stop();
    }
}
