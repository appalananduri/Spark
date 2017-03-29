package com.venkat.MapRDD;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;


public class ExceptMinusRDD {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("Minus Sparl RDD") ;
		JavaSparkContext sc = new JavaSparkContext(conf) ;
		sc.setLogLevel("WARN") ;
		
		System.out.println("Spark Version " + sc.version());
		
		JavaRDD<String> orderFile1 = sc.textFile(args[0]) ;
		JavaRDD<String> orderFile2 = sc.textFile(args[1]); 
		
		JavaRDD<String> MinusOrder = orderFile1.subtract(orderFile2) ;
		
		System.out.println("Number of records " + MinusOrder.count());
		
		MinusOrder.saveAsTextFile(args[2]) ;
		
		sc.close() ;
		

	}

}
