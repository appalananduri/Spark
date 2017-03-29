package com.venkat.MapRDD;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.hive.HiveContext;

public class DFOrderZscore {


	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("Order Spark SQL") ;
		JavaSparkContext sc = new JavaSparkContext(conf) ;
		
		HiveContext sqlCtx = new HiveContext(sc.sc()) ;
		
		System.out.println("Spark version is " + sc.version());
		
		sc.setLogLevel("WARN") ;
	
				
		sqlCtx.sql("use vac_work") ;
		
		DataFrame cntDF = sqlCtx.sql("  select ordr_plcmt_dt, ordr_buy_sell_cd, buyer_sellr_intrl_id, last_ordr_unit_qt, " + 
  " avg(last_ordr_unit_qt) over (partition by ordr_buy_sell_cd) runavg, " + 
  " stddev(last_ordr_unit_qt) over (partition by ordr_buy_sell_cd) stddev_qt " +
  " from myorder ") ;
		
/*		DataFrame cntDF = sqlCtx.sql("  select ordr_plcmt_dt, ordr_buy_sell_cd, buyer_sellr_intrl_id, last_ordr_unit_qt, " + 
				  " lag(last_ordr_unit_qt,1,0) over (order by last_ordr_unit_qt) lagvalue,  " +
				  " lead(last_ordr_unit_qt,1,0) over (order by last_ordr_unit_qt) leadvalue  " +
				  " from myorder ") ;	
		
*/
	  cntDF.write().mode(SaveMode.Overwrite).saveAsTable("orderfeature2") ;	
		
	

		
		System.out.println("done!");
		sc.close() ;
		

	}

}
