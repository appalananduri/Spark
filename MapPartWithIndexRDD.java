package com.venkat.MapRDD;
import java.util.Iterator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;


public class MapPartWithIndexRDD {

	
	public static void main(String[] args) {
	
		SparkConf conf = new SparkConf().setAppName("Spark Map Partition with Index") ;
		JavaSparkContext sc = new JavaSparkContext(conf) ;
		
		JavaRDD<String> myFile = sc.textFile(args[0]) ;
		
		JavaRDD<String> orderMapPartIndex = myFile.mapPartitionsWithIndex(
			new Function2<Integer, Iterator<String>, Iterator<String>>() {

				public Iterator<String> call(Integer partition, Iterator<String> recSet)
			    {
				   if(partition == 0 && recSet.hasNext()) {
					   recSet.next() ; // move pointer to next record in set
					   return recSet ;
				   }
				   else return recSet ;
				}
			}
			, false	) ;
		
		orderMapPartIndex.saveAsTextFile(args[1]) ;
		
		sc.close() ;
	
	}

}
