package com.venkat.MapRDD;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class MapPairRDDCnt {

	public static void main(String[] args) {

		// Define SparkConf and JavaSparkContext
		SparkConf conf = new SparkConf().setAppName("My Map Pair RDD") ;
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		// set Log level to Warn
		// Open text file 
		
		sc.setLogLevel("WARN") ;
		
		JavaRDD<String> order = sc.textFile(args[0]) ;
		
		System.out.println("Spark Version is " + sc.version());
		System.out.println("Input file is " + args[0]);
		System.out.println("Output Dir is " + args[1]);
		
		// use Map to eliminate unwanted columns
		JavaRDD<String> orderMap = order.map(
				new Function<String, String>() {
					public String call (String orderRec) {
						String[] orderColumns = orderRec.split(",") ;
						return orderColumns[2]+","+orderColumns[4] ;
					}	
				}		
		);
		
		// use map pair to form key , value pair
		JavaPairRDD<String, Integer> accountPair = orderMap.mapToPair(
			new PairFunction<String, String, Integer>() {
				public Tuple2<String, Integer> call (String record) {
					String[] columns = record.split(",") ;
					return new Tuple2<String, Integer> (columns[1], Integer.parseInt("1")) ;
				}
			}
	     );
		
		JavaPairRDD<String, Integer> accountCnt = accountPair.reduceByKey(
			new Function2<Integer, Integer, Integer>() {
				public Integer call(Integer value1, Integer value2) {
					return (value1 + value2) ;
				}
			}
		);
		
		JavaRDD<String> outputFile = accountCnt.map(
				new Function<Tuple2<String, Integer> , String >() {
					public String call(Tuple2<String, Integer> arg0) {
						String rec ;
						rec = arg0._1 + "," + arg0._2.toString() ;
						return rec;
					}
				}
				) ;
		
		
		outputFile.saveAsTextFile(args[1]) ;
		
		// close JavaSpark Context
		sc.close() ;
	}
}
