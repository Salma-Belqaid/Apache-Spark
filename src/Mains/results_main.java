package Mains;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.SparkConf;



public class results_main {
	public static void main(String[] args) {
		System.setProperty("hadoop.home.dir", "C:\\hadoop");

		SparkConf conf = new SparkConf()
				.setAppName("Spark")
				.setMaster("local");
		
		SparkSession spark = SparkSession.builder().config(conf).getOrCreate();
		
		Dataset<Row> csvDataset = spark.read()
				.format("csv")
				.option("header", "true")
				.load("results.csv");
		
		csvDataset.createOrReplaceTempView("csvdataTable");
		
		System.out.println("***************** Display how many result inside of the dataset *****************");
		Dataset<Row> nobDataset = spark.sql("select COUNT(*) from csvdataTable ").toDF();
		nobDataset.show();
		
		System.out.println("************ Display the firste 5 results of dataset ************");
		Dataset<Row> reducedCSVDataset = spark.sql("select * from csvdataTable Limit 5").toDF();
		reducedCSVDataset.show();
		
		System.out.println("************ filter results by home_score = 0 ************");
		Dataset<Row> filterCSVDataset = spark.sql("select * from csvdataTable where home_score='0' ").toDF();
		filterCSVDataset.show();
		
		System.out.println("********************* Display the Home_team that have the highest score in Germany ************************");
		Dataset<Row> maxCSVDataset = spark.sql("select date,home_team,home_score from csvdataTable where home_score = (select max(home_score) from csvdataTable)").toDF();
		maxCSVDataset.show();
		
		System.out.println( "****************** count Results ho hase neutra(Whether the match took place at a neutral venue or not)=True ***************************");
		Dataset<Row> countCSVDataset = spark.sql("select COUNT(neutral) from csvdataTable where neutral='TRUE' ").toDF();
		countCSVDataset.show();
		
		System.out.println("******************* Display the results of match ho hase home_score=Abkhazia, sorted by away_score **************************");
		Dataset<Row> sortedCSVDataset = spark.sql("select date,home_team,away_score from csvdataTable where home_team='Abkhazia' order by away_score ASC").toDF();
		sortedCSVDataset.show();
		
		System.out.println("******************* Count the results of match ho hase home_team=Germany, and home_score=0 **************************");
		Dataset<Row> count2CSVDataset = spark.sql("select COUNT(home_team) from csvdataTable where home_team='Germany' and home_score='0' ").toDF();
		count2CSVDataset.show();

	}
}
