package Mains;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class results_women_main 
{
	public static void main(String[] args) {
		System.setProperty("hadoop.home.dir","C:\\hadoop" );

		SparkConf conf=new SparkConf().setAppName("Spark").setMaster("local");
		        SparkSession spark = SparkSession.builder().config(conf)
                .getOrCreate();
Dataset<Row> csvDataset = spark.read().format("csv").option("header", "true")
                .load("results1.csv");
csvDataset.createOrReplaceTempView("csvdataTable");
Dataset<Row> reducedCSVDataset = spark.sql("select * from csvdataTable limit 1").toDF();

Long x;
reducedCSVDataset.show();

reducedCSVDataset = spark.sql("select * from csvdataTable where home_team='Vietnam' and home_score>2").toDF();
reducedCSVDataset.show();

reducedCSVDataset = spark.sql("select * from csvdataTable where home_team='Hong Kong' and home_score>away_score").toDF();
reducedCSVDataset.show();

reducedCSVDataset = spark.sql("select * from csvdataTable where home_team='Hong Kong' and home_score>away_score and country='Hong Kong'").toDF();
reducedCSVDataset.show();
	
reducedCSVDataset = spark.sql("select * from csvdataTable where home_team='Hong Kong' and home_score>away_score and country!='Hong Kong'").toDF(); x=reducedCSVDataset.count();
x=reducedCSVDataset.count();
System.out.println(x);


reducedCSVDataset = spark.sql("select home_team,away_team,home_score,away_score from csvdataTable where home_score-away_score>2").toDF();
reducedCSVDataset.show();

reducedCSVDataset = spark.sql("select home_team from csvdataTable where home_team='Italy' and home_score>away_score and tournament='Euro' and date LIKE '%1969%'").toDF();
reducedCSVDataset.show();
x=reducedCSVDataset.count();
System.out.println(x);

reducedCSVDataset = spark.sql("select date,home_team,away_team,home_score,away_score,neutral from csvdataTable where date LIKE '%1977%' and neutral='TRUE'").toDF();
reducedCSVDataset.show();


}
}

