package Mains;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Suicides_main {
	public static void main(String[] args) {
		System.setProperty("hadoop.home.dir", "C:\\hadoop");

		SparkConf conf = new SparkConf()
				.setAppName("Spark")
				.setMaster("local");
		
		SparkSession spark = SparkSession.builder().config(conf).getOrCreate();
		
		Dataset<Row> csvDataset = spark.read()
				.format("csv")
				.option("header", "true")
				.load("Suicides_in_India .csv");
		
csvDataset.createOrReplaceTempView("csvdataTable");

System.out.println("show how many results");
Dataset<Row> reducedCSVDataset = spark.sql("select * from csvdataTable ").toDF();
reducedCSVDataset.show();
Long x  = reducedCSVDataset.count();
System.out.println("there are "+x+" results in this file ");

System.out.println("Display all major suicides");
reducedCSVDataset = spark.sql("select type_code,COUNT(State) from csvdataTable GROUP BY type_code ").toDF();
reducedCSVDataset.show();

System.out.println("Display all minor suicides");
reducedCSVDataset = spark.sql("select type,COUNT(State) from csvdataTable GROUP BY type").toDF();
reducedCSVDataset.show();
	
System.out.println("Display Women suicides in 2001");
reducedCSVDataset = spark.sql("select COUNT(State) from csvdataTable where Gender='Female' and Year='2001' GROUP BY Gender").toDF();
reducedCSVDataset.show();

System.out.println("Display Max suicides between 15-29");
reducedCSVDataset = spark.sql("select * from csvdataTable where Age_group='15-29' ORDER BY CAST(Total AS Integer) DESC Limit 1").toDF();
reducedCSVDataset.show();

System.out.println("Display 10 male who suiicede by drowning in 2004");
reducedCSVDataset = spark.sql("select * from csvdataTable where Gender='Male' and Type='By Drowning' and Year='2004' Limit 10").toDF();
reducedCSVDataset.show();

System.out.println("Display Minimum total male between 30-44 ");
reducedCSVDataset = spark.sql("select * from csvdataTable where Age_group='30-44' and Gender='Male' ORDER BY Total ASC limit 1 ").toDF();
reducedCSVDataset.show();




	
	}
}

