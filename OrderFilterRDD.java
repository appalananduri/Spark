package com.venkat.MapRDD;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

public class OrderFilterRDD {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("Order Filter") ;
		JavaSparkContext sc = new JavaSparkContext(conf) ;
		System.out.println("Spark version is : " + sc.version());
		
		sc.setLogLevel("WARN") ;
		
		JavaRDD<String> myFile = sc.textFile(args[0]) ;
		
		JavaRDD<String> orderFilter = myFile.filter(
				new Function<String, Boolean>() {

					public Boolean call(String orderRec)  {
						String cols[] = orderRec.split(",") ;
						if (cols[13].toUpperCase().equals("USD"))
						 return true ;
						
						return false ;
					}
					
				}
				) ;
		
		
		
		if (!orderFilter.isEmpty() ) {
			  System.out.println("Number of records " + orderFilter.count() );
			   }	
		else
			System.out.println("No records found....");
		
		
		sc.close();

	}

}
