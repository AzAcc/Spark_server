package com.company;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Hdfs;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.sql.types.DataTypes;
import org.spark_project.dmg.pmml.True;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class TestCLass {
    public static void main(String[] args) throws IOException {
        long startTime = System.nanoTime();

        Configuration conf = new Configuration();
        FileSystem  hdfs = FileSystem.get(URI.create("hdfs://cdh631.itfbgroup.local:8020/user/usertest/AZ/Suc-user"), conf);
        Path path = new Path("hdfs://cdh631.itfbgroup.local:8020/user/usertest/AZ/Suc-user");
        if (hdfs.exists(path)){
            hdfs.delete(path,true);
        }

        Configuration conf1 = new Configuration();
        FileSystem  hdfs1 = FileSystem.get(URI.create("hdfs://cdh631.itfbgroup.local:8020/user/usertest/AZ/Suc_user_sql"), conf);
        Path path1 = new Path("hdfs://cdh631.itfbgroup.local:8020/user/usertest/AZ/Suc_user_sql");
        if (hdfs.exists(path1)){
            hdfs.delete(path1,true);
        }

        String warehouseLocation = new File("hdfs://cdh631.itfbgroup.local:8020/user/hive/warehouse").getAbsolutePath();

        SparkSession sparkSession = SparkSession
                .builder()
                .appName("Success")
                .master("local")
                .getOrCreate();

        SparkSession hive_sparkSession = SparkSession
                .builder()
                .appName("Java Spark Hive Example")
                .config("spark.sql.warehouse.dir", warehouseLocation)
                .enableHiveSupport()
                .getOrCreate();

        Dataset<Row> dataset = sparkSession
                .read()
                .format("csv")
                .option("header", "true")
                .load("hdfs://cdh631.itfbgroup.local:8020/user/usertest/event_data_train/event_data_train.csv");

                long count_steps = dataset.select("step_id").distinct().count();

                Dataset<Row> dataset1 = dataset.select("step_id","action","user_id")
                        .where(dataset.col("action").like("passed"))
                        .groupBy("user_id")
                        .count();

                dataset1.where(String.format("count= %d",count_steps))
                        .drop("count")
                        .repartition(1)
                        .write()
                        .format("csv")
                        .option("header",true)
                        .save("hdfs://cdh631.itfbgroup.local:8020/user/usertest/AZ/Suc-user");

       /* hive_sparkSession.sql("CREATE TABLE IF NOT EXISTS az.event_data_train " +
                "(step_id int,time_stamp int,action string,user_id int) " +
                "row format delimited " +
                "fields terminated by ',' " +
                "USING hive");
        hive_sparkSession.sql("LOAD DATA INPATH '/user/usertest/event_data_train/event_data_train.csv' into table az.event_data_train");
        /*hive_sparkSession.sql("create table all_steps from(select distinct step_id from az.event_data_train)");
        hive_sparkSession.sql("select * from(SELECT user_id,count(*) as passed_steps from az.event_data_train where action =\\\"passed\\\" group by user_id))" +
                "where passed_steps = (select count(*) from all_steps");
        hive_sparkSession.sql("DROP table all_steps");
        hive_sparkSession.sql("DROP table event_data_train")
                .repartition(1)
                .write()
                .format("csv")
                .option("header",true)
                .save("hdfs://cdh631.itfbgroup.local:8020/user/usertest/AZ/Suc_user_sql");
        */
        sparkSession.stop();
        hive_sparkSession.stop();

        long endTime   = System.nanoTime();
        long totalTime = endTime - startTime;
        System.out.println(totalTime);
    }
}


