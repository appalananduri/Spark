package com.venkat.MapRDD;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.api.java.UDF3;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.sql.types.DataTypes;

public class OrderTRLDataFrame {


	public static void main(String[] args) {
		SparkConf conf = new SparkConf() ;
		JavaSparkContext sc = new JavaSparkContext(conf) ;
		
		HiveContext sqlCtx = new HiveContext(sc.sc()) ;
		
		sc.setLogLevel("WARN") ;
		System.out.println(sc.version());
		
		sqlCtx.sql("use venkat") ;
		
		
		UDF3<String, Integer, Integer, String> substr = 
				new UDF3<String, Integer, Integer,String>() {

					public String call(String arg0, Integer arg1,
							Integer arg2) throws Exception {
						
						return arg0.substring(arg1, arg2) ;
					}

	     } ;
	     
	     sqlCtx.udf().register("substr", substr, DataTypes.StringType) ;
		
		JavaRDD<String> orderFile = sc.textFile(args[0]) ;
		

		
		JavaRDD<OrderTRL> orderMap = orderFile.map(
			new Function<String, OrderTRL>() {
				public OrderTRL call (String rec) {
					OrderTRL order = new OrderTRL() ;
					String cols[] = rec.split(",") ;
					
					order.setORDR_INTRL_ID(cols[0]) ;
					order.setORDR_PLCMT_DT(cols[1]) ;
					order.setSCRTY_INTRL_ID(cols[2]) ;
					order.setORDR_BUY_SELL_CD(cols[3]) ;
					order.setLAST_ORDR_UNIT_QT(Double.parseDouble(cols[4])) ;
					order.setBUYER_SELLR_INTRL_ID(cols[5]) ;
					order.setBUYER_SELLR_ACCT_TYPE_CD(cols[6]) ;
					
					return order ;
				}
			}
		) ;
		
		
		DataFrame OrderTRL = sqlCtx.createDataFrame(orderMap, OrderTRL.class) ;
		OrderTRL.registerTempTable("Order") ;
		
		DataFrame orderData = sqlCtx.sql(
				"Select ORDR_INTRL_ID as OrderId, substr(ORDR_INTRL_ID,1,3) as SourceSystem, " +
				" SCRTY_INTRL_ID as SecurityId, ORDR_BUY_SELL_CD as OrderBuySellCd,LAST_ORDR_UNIT_QT as OrderQty, " +
				" BUYER_SELLR_INTRL_ID as OrderAcctId, BUYER_SELLR_ACCT_TYPE_CD OrderAcctTypeCd FROM Order") ;
		
		DataFrame orderAgg = orderData.groupBy("SecurityId").
			agg(org.apache.spark.sql.functions.sum(orderData.col("OrderQty")).alias("SumOrderQty")
			,org.apache.spark.sql.functions.max(orderData.col("OrderQty")).alias("MaxOrderQty")
			,org.apache.spark.sql.functions.min(orderData.col("OrderQty")).alias("MinOrderQty")
			,org.apache.spark.sql.functions.stddev(orderData.col("OrderQty")).alias("StdDevOrderQty")
			,org.apache.spark.sql.functions.count("SecurityId").alias("RowCount")
			  ) ;

	//	orderAgg.rdd().saveAsTextFile(args[1]) ;
		orderAgg.write().mode(SaveMode.Overwrite).saveAsTable("orderagg") ;
		OrderTRL.write().mode(SaveMode.Overwrite).saveAsTable("myorder") ;
		
		sc.close() ;
		
		System.out.println("Done!");
	}
}
