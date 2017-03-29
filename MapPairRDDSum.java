package com.venkat.MapRDD;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class MapPairRDDSum {

	public static void main(String[] args) {

		SparkConf conf = new SparkConf().setAppName("Account Map Pair sum") ;
	
		
		JavaSparkContext sc = new JavaSparkContext(conf) ;
		
		System.out.println("Spark Version is " + sc.version());
		System.out.println("Input file is " + args[0]);
		System.out.println("Output dir is " + args[1]);
		
		sc.setLogLevel("WARN") ;
		
		JavaRDD<String> order = sc.textFile(args[0]) ;
		
		JavaRDD<String> accountMap = order.map(
			new Function<String, String>() {
				public String call (String orderRec) {
					String[] cols = orderRec.split(",") ;
					return cols[1]+","+cols[2] ;
				}
			}
		) ;
		
		JavaPairRDD<String, Double> accountPair = accountMap.mapToPair(
			new PairFunction<String, String, Double>() {
				public Tuple2<String, Double> call (String rec) {
					String[] cols = rec.split(",") ;
					return new Tuple2<String, Double>(cols[0], Double.parseDouble(cols[1])) ;
				}
			}
		) ;
		
		JavaPairRDD<String, Double> accountSum = accountPair.reduceByKey(
			new Function2<Double, Double, Double>() {
				public Double call (Double value1, Double value2) {
				return (value1+value2) ;	
				}
			}
		);
		
		accountSum.saveAsTextFile(args[1]) ;
		sc.close() ;
	}

}
