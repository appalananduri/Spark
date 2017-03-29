package com.venkat.MapRDD;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class IntersectionRDD {


	public static void main(String[] args) {
		SparkConf conf = new SparkConf() ;
		JavaSparkContext sc = new JavaSparkContext(conf);
		sc.setLogLevel("WARN") ;
		
		System.out.println("Spark Version " + sc.version());
		
		JavaRDD<String> order1File = sc.textFile(args[0]) ;
		JavaRDD<String> order2File = sc.textFile(args[1]) ;
		
		JavaRDD<String> commonOrderRecs = order2File.intersection(order1File) ;
		
		commonOrderRecs.saveAsTextFile(args[2]) ;
		System.out.println("Nuber of records " + commonOrderRecs.count() );
		
		
		sc.close() ;
	}

}
