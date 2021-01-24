package com.sparkTutorial.mylearning.dataset;

import com.sparkTutorial.mylearning.dataset.javabeans.NumberEngToFre;
import com.sparkTutorial.mylearning.dataset.javabeans.Point;
import com.sparkTutorial.mylearning.dataset.javabeans.Segment;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;
import java.util.List;

import static org.apache.spark.sql.functions.col;

public class SparkSql004ComplexTypeNestedJavaBean {
    public static void main(String[] args) {
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        SparkSession spark = SparkSession.builder().appName("sparksql004").master("local[*]")
                .config("spark.sql.warehouse" , "file:///c:tmp" ) //only for windows
                .getOrCreate();
        System.out.println("*** Example 1: nested Java beans");

        Encoder<Segment> segmentEncoder = Encoders.bean(Segment.class);

        List<Segment> data = Arrays.asList(
                new Segment(new Point(1.0, 2.0), new Point(3.0, 4.0)),
                new Segment(new Point(8.0, 2.0), new Point(3.0, 14.0)),
                new Segment(new Point(11.0, 2.0), new Point(3.0, 24.0)));

        Dataset<Segment> ds = spark.createDataset(data, segmentEncoder);
        System.out.println("*** here is the schema inferred from the bean");
        ds.printSchema();
        System.out.println("*** here is the data");
        ds.show();
        //Use the convenient bean-inferred column names to query
        System.out.println("*** filter by one column and fetch others");
        ds.where(col("from").getField("x").geq(8.0) )
          .where(col("from").getField("y").geq(2.0))
          .select(col("to")).show();

        //ds.where(col("from").getField("x").gt(7.0)).select(col("to")).show();
        spark.close();

    }
}
