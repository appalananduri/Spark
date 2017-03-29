package com.venkat.MapRDD;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;


public class MapPartRDD implements Serializable {

	public static void main(String[] args) {

		
		SparkConf conf = new SparkConf().setAppName("Spark Map Partition") ;
		JavaSparkContext sc = new JavaSparkContext(conf) ;
		sc.setLogLevel("WARN") ;
		
		JavaRDD<String> myfile = sc.textFile(args[0],2) ;
		JavaRDD<String> orderMP  = myfile.mapPartitions(
				 new FlatMapFunction<Iterator<String>, String>() {

					public Iterable<String> call(Iterator<String> recSet)
			    	{
					 List<String> recs = new ArrayList<String>() ;
					 while (recSet.hasNext()) {
						 String rec = recSet.next() ;
						 String cols[] = rec.split(",") ;
						 recs.add(cols[0]+","+cols[2]) ;
					 }
						return recs;
					} 
				 }
		) ;
		
        orderMP.saveAsTextFile(args[1]) ;		
		
		sc.close() ;
	}

}
