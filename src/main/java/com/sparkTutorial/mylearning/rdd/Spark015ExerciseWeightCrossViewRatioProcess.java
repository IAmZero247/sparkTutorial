package com.sparkTutorial.mylearning.rdd;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

public class Spark015ExerciseWeightCrossViewRatioProcess {
    public static void main(String[] args) {
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        SparkConf conf = new SparkConf().setAppName("course-analysis").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        boolean testMode = false;
        //courseId,title
        JavaPairRDD<Integer, String> titlesData = getTitlesDataRdd(sc, testMode);
        //chapterId,courseId
        JavaPairRDD<Integer, Integer> chapterData = getChapterDataRdd(sc, testMode);
        //userId,chapterId,dateAndTime
        JavaPairRDD<Integer, Integer> viewData = getViewDataRdd(sc, testMode);


        /*objective1 -> aggregate number of chapter in each course*/
        JavaPairRDD<Integer, Integer> chapterAggregateWrtTitle = chapterData.mapToPair(i -> new Tuple2<Integer, Integer>(i._2,1))
                   .reduceByKey((val1, val2) -> val1+val2);

        /*
        Exercise 2: Your job is to produce a ranking chart detailing which are the
        most popular courses by score.
        Business Rules:
        We think that if a user sticks it through most of the course, that's more
        deserving of "points" than if someone bails out just a quarter way through the
        course. So we've cooked up the following scoring system:
         If a user watches more than 90% of the course, the course gets 10 points
         If a user watches > 50% but <90% , it scores 4
         If a user watches > 25% but < 50% it scores 2
         Less than 25% is no score
         */
        //Step 1 - remove duplicate row from same user for same chapter
        viewData =viewData.distinct();
        //Step 2: Joining to get Course Id in the RDD
        JavaPairRDD<Integer, Tuple2<Integer, Integer>> viewDataJoinedToChapter = viewData.mapToPair(i -> i.swap()).join(chapterData);
        //Step 3: chapter ids are irrelvant , prepare for reduce
        JavaPairRDD<Tuple2<Integer, Integer>, Integer> preparingForReduce = viewDataJoinedToChapter.mapToPair(row -> {
            Integer userId = row._2._1;
            Integer courseId = row._2._2;
            return new Tuple2<Tuple2<Integer, Integer>, Integer>(new Tuple2<Integer, Integer>(userId, courseId), 1);

        });

        JavaPairRDD<Tuple2<Integer, Integer>, Integer> userCourseAndAggregatedView = preparingForReduce.reduceByKey((value1, value2) -> value1+value2);
        // Step 5 - remove the userIds
        JavaPairRDD<Integer, Integer>  filteredUserFromUserCourseAndAggregatedView= userCourseAndAggregatedView.mapToPair(i -> new Tuple2<Integer, Integer>(i._1._2, i._2));
        // Step 6 - add in the total chapter count -> join
        JavaPairRDD<Integer, Tuple2<Integer, Integer>> courseAndAggregatedViewWithAggregatedChapters = filteredUserFromUserCourseAndAggregatedView.join(chapterAggregateWrtTitle);
        // Step 7 - convert to percentage
        //JavaPairRDD<Integer, Double> viewInPercentage = courseAndAggregatedViewWithAggregatedChapters.mapToPair(i -> new Tuple2<Integer, Double>(i._1, (double) Math.round(i._2._1 * 100  / i._2._2)  ));
        JavaPairRDD<Integer, Double> viewInPercentage = courseAndAggregatedViewWithAggregatedChapters.mapValues(i ->  (double) Math.round(i._1 * 100  / i._2)  );
        // Step 8 - convert to scores
        JavaPairRDD<Integer, Integer> viewsInScore = viewInPercentage.mapValues(i -> Spark015ExerciseWeightCrossViewRatioProcess.getPoints(i));
        // Step 9 - reduce scores
        JavaPairRDD<Integer, Integer> reducedScores = viewsInScore.reduceByKey((val1, val2) -> val1 + val2);
        // Step 10
        JavaPairRDD<Integer, Tuple2<Integer, String>> reducedScoredWithTitles = reducedScores.join(titlesData);
        // Step 11  Rearranged for sorting with score
        //JavaPairRDD<Integer, Tuple2<Integer, String>> formattedOut =  reducedScoredWithTitles.mapToPair( i-> new Tuple2<Integer, Tuple2<Integer, String>>(i._2._1, new Tuple2(i._1, i._2._2)));
        JavaPairRDD<Integer, String> formattedOut = reducedScoredWithTitles.mapToPair(row -> new Tuple2<Integer, String>(row._2._1, row._2._2));
        formattedOut.sortByKey(false).collect().forEach(i -> System.out.println(i));


        Scanner scan = new Scanner(System.in);
        scan.nextLine();
        sc.close();
    }

    public static int getPoints(Double value ){
        if (value > 90) return 10;
        if (value > 50) return 4;
        if (value > 25) return 2;
        return 0;
    }

    public static JavaPairRDD<Integer, Integer> getChapterDataRdd (JavaSparkContext sc , Boolean testMode){
        if (testMode)
        {
            // Chapter views - (userId, chapterId)
            List<Tuple2<Integer, Integer>> rawChapterData = new ArrayList<>();
            rawChapterData.add(new Tuple2<>(96,  1));
            rawChapterData.add(new Tuple2<>(97,  1));
            rawChapterData.add(new Tuple2<>(98,  1));
            rawChapterData.add(new Tuple2<>(99,  2));
            rawChapterData.add(new Tuple2<>(100, 3));
            rawChapterData.add(new Tuple2<>(101, 3));
            rawChapterData.add(new Tuple2<>(102, 3));
            rawChapterData.add(new Tuple2<>(103, 3));
            rawChapterData.add(new Tuple2<>(104, 3));
            rawChapterData.add(new Tuple2<>(105, 3));
            rawChapterData.add(new Tuple2<>(106, 3));
            rawChapterData.add(new Tuple2<>(107, 3));
            rawChapterData.add(new Tuple2<>(108, 3));
            rawChapterData.add(new Tuple2<>(109, 3));
            return sc.parallelizePairs(rawChapterData);
        }

        JavaRDD<String> rdd= sc.textFile("in/course_view_analysis/chapters.csv");
        JavaPairRDD<Integer, Integer> courseRdd = rdd.mapToPair(commaSeparatedLine -> {
            String[] cols = commaSeparatedLine.split(",");
            return new Tuple2<>(Integer.valueOf(cols[0]), Integer.valueOf(cols[1]));
        }).persist(StorageLevel.MEMORY_AND_DISK()); //refined using spark-ui
        return courseRdd;
    }


    public static JavaPairRDD<Integer, String> getTitlesDataRdd (JavaSparkContext sc , Boolean testMode){
        if (testMode)
        {
            // (chapterId, title)
            List<Tuple2<Integer, String>> rawTitles = new ArrayList<>();
            rawTitles.add(new Tuple2<>(1, "How to find a better job"));
            rawTitles.add(new Tuple2<>(2, "Work faster harder smarter until you drop"));
            rawTitles.add(new Tuple2<>(3, "Content Creation is a Mug's Game"));
            return sc.parallelizePairs(rawTitles);
        }
        JavaRDD<String> rdd= sc.textFile("in/course_view_analysis/titles.csv");
        JavaPairRDD<Integer, String> titlesRdd = rdd.mapToPair(commaSeparatedLine -> {
            String[] cols = commaSeparatedLine.split(",");
            return new Tuple2<>(Integer.valueOf(cols[0]), cols[1]);
        });
        return titlesRdd;
    }

    public static JavaPairRDD<Integer, Integer> getViewDataRdd (JavaSparkContext sc , Boolean testMode){
        if (testMode)
        {
            // Chapter views - (userId, chapterId)
            List<Tuple2<Integer, Integer>> rawViewData = new ArrayList<>();
            rawViewData.add(new Tuple2<>(14, 96));
            rawViewData.add(new Tuple2<>(14, 97));
            rawViewData.add(new Tuple2<>(13, 96));
            rawViewData.add(new Tuple2<>(13, 96));
            rawViewData.add(new Tuple2<>(13, 96));
            rawViewData.add(new Tuple2<>(14, 99));
            rawViewData.add(new Tuple2<>(13, 100));
            return  sc.parallelizePairs(rawViewData);
        }

        return sc.textFile("in/course_view_analysis/views-*.csv")
                .mapToPair(commaSeparatedLine -> {
                    String[] columns = commaSeparatedLine.split(",");
                    return new Tuple2<Integer, Integer>(new Integer(columns[0]), new Integer(columns[1]));
                });
    }
}
