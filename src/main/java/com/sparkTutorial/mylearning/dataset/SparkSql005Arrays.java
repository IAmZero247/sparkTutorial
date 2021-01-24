package com.sparkTutorial.mylearning.dataset;

import com.sparkTutorial.mylearning.dataset.javabeans.Line;
import com.sparkTutorial.mylearning.dataset.javabeans.Point;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;
import java.util.List;

import static org.apache.spark.sql.functions.*;

public class SparkSql005Arrays {
    public static void main(String[] args) {
        Encoder<Line> lineEncoder = Encoders.bean(Line.class);
        List<Line> lines = Arrays.asList(
                new Line("a", new Point[]{new Point(0.0, 0.0), new Point(2.0, 4.0)}),
                new Line("b", new Point[]{new Point(-1.0, 0.0)}),
                new Line("c", new Point[]
                        {new Point(0.0, 0.0), new Point(2.0, 6.0), new Point(10.0, 100.0)})
        );

        Logger.getLogger("org.apache").setLevel(Level.WARN);
        SparkSession spark = SparkSession.builder().appName("sparksql005").master("local[*]")
                .config("spark.sql.warehouse" , "file:///c:tmp" ) //only for windows
                .getOrCreate();
        System.out.println("*** Example 1: arrays");

        Dataset<Line> linesDS = spark.createDataset(lines, lineEncoder);

        System.out.println("*** here is the schema inferred from the bean");
        linesDS.printSchema();

        System.out.println("*** here is the data");
        linesDS.show();

        // notice here you can filter by the second element of the array, which
        // doesn't even exist in one of the rows
        System.out.println("*** filter by an array element");
        linesDS
                .where(col("points").getItem(1).getField("y").gt(3.0))
                .select(col("name"), size(col("points")).as("count")).show();
        spark.close();

    }
}
