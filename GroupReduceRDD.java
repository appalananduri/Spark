package com.venkat.MapRDD;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class GroupReduceRDD {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("Reduce and Group By key") ;
		JavaSparkContext sc = new JavaSparkContext(conf) ;
		
		sc.setLogLevel("WARN") ;
		System.out.println("Spark Version is " + sc.version());
		
		JavaRDD<String> file1 = sc.textFile(args[0]) ;
		
		JavaRDD<String> orderAcct = file1.map(
			new Function<String, String>() {
				public String call (String rec) {
					String cols[] = rec.split(",") ;
					return cols[2]+","+cols[4] ;
					
				}
			}
		) ;
		
		JavaPairRDD<String, Double> acctPair = orderAcct.mapToPair(
		  new PairFunction<String, String, Double>() {
			public Tuple2<String, Double> call(String rec) throws Exception {
				String cols[] = rec.split(",") ;
				return new Tuple2<String, Double>(cols[0], Double.parseDouble(cols[1])) ;
			}
		  }	
		) ;
		
		JavaPairRDD<String, Double> acctSum = acctPair.aggregateByKey(
			0.0,
			new Function2<Double, Double, Double>() {

				public Double call(Double a, Double b) throws Exception {
					return (a+b);
				}
			},
			
			new Function2<Double, Double, Double>() {

				public Double call(Double arg0, Double arg1) throws Exception {
					// TODO Auto-generated method stub
					return arg0+arg1;
				}
			}
	
		) ;

		
		acctSum.saveAsTextFile(args[1]) ;
		
		sc.close() ;

	}

}
