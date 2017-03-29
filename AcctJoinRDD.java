package com.venkat.MapRDD;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class AcctJoinRDD {


	protected Double _2;

	public static void main(String[] args) {

		SparkConf conf = new SparkConf().setAppName("Account Join RDD") ;
		JavaSparkContext sc = new JavaSparkContext(conf) ;
		
		sc.setLogLevel("WARN") ;
		
		System.out.println("Spark Version is " + sc.version());
		System.out.println("Input file is " + args[0]);
		
		JavaRDD<String> order = sc.textFile(args[0]) ;
		
		JavaRDD<String> account = order.map(
			new Function<String, String>() {
				public String call (String accountRec) {
					String[] col = accountRec.split(",") ;
					return col[2]+","+col[4] ;
				}
			}
		) ;
		
		System.out.println(account.first()) ;
		
		JavaPairRDD<String, Integer> accountPair = account.mapToPair(
				new PairFunction<String, String, Integer>() {
					public Tuple2<String, Integer> call (String rec) {
						String[] col = rec.split(",") ;
						return new Tuple2<String, Integer>(col[0], Integer.parseInt("1") );
					}
				}
		) ;
		
		
		JavaPairRDD<String, Integer> accountCnt = accountPair.reduceByKey(
			new Function2<Integer, Integer, Integer>() {
				public Integer call(Integer a, Integer b) throws Exception {
					return a+b ;
				}
			}
		) ;
		
		
   	   JavaPairRDD<String, String> accountRecPair = order.mapToPair(				new PairFunction<String, String, String>() {
					public Tuple2<String, String> call (String rec) {
						String[] col = rec.split(",") ;
						return new Tuple2<String, String>(col[2], rec );
					}
				}
		) ;
		
		JavaPairRDD<String, Double> accountSumPair = account.mapToPair(
				new PairFunction<String, String, Double>() {
					public Tuple2<String, Double> call (String rec) {
						String[] col = rec.split(",") ;
						return new Tuple2<String, Double>(col[0], Double.parseDouble(col[1]) );
					}
				}
		) ;
		
		JavaPairRDD<String, Double> accountSum = accountSumPair.reduceByKey(
				new Function2<Double, Double, Double>() {
					public Double call(Double a, Double b) throws Exception {
						return a+b ;
					}
				}
			) ;	
		
	JavaPairRDD<String, Tuple2<Tuple2<String, Integer>, Double>> accountNew = accountRecPair.join(accountCnt).join(accountSum) ;
		

    JavaRDD<String> outputFile = accountNew.map(
    	new Function< Tuple2<String, Tuple2<Tuple2<String, Integer>, Double>>, String>() {

			public String call(
					Tuple2<String, Tuple2<Tuple2<String, Integer>, Double>> rec)
					 {
				        String orderRec ;
				      //  orderRec = rec._1 ;
				        Tuple2<Tuple2<String, Integer>, Double> rec1 = rec._2() ;
				        Tuple2<String, Integer> rec2 = rec1._1() ;
				        
				        orderRec = rec2._1 + "," + rec2._2().toString() + "," + rec1._2().toString() ;

				        return orderRec;
			          }    		
    	}
    ) ;
	
    outputFile.saveAsTextFile(args[1]) ;	
	
	System.out.println(outputFile.first() ) ;
	sc.close() ;

	
	}

}
