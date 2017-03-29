package com.venkat.MapRDD;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;


public class SparkMapRDD {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("My Spark  RDD") ;
		
		// define Java spark context which takes spark conf as parameter
		JavaSparkContext sc = new JavaSparkContext(conf) ;
		sc.setLogLevel("WARN") ;
		
		System.out.println("Spark Version is " + sc.version());
		System.out.println("Input file is " + args[0]);
		System.out.println("output file is " + args[1]);
		
		JavaRDD<String> myFile = sc.textFile(args[0]) ;
		
		JavaRDD<String> myMapRDD = myFile.map(
				// Function<input, output>
				new Function<String, String>() {
					public String call(String fileRecord) {
						String columns[] = fileRecord.split(",") ;
						return columns[0]+","+columns[2];
					}
				}
				) ;
		
		myMapRDD.saveAsTextFile(args[1]) ;
		
		sc.close() ;

		}


	}


