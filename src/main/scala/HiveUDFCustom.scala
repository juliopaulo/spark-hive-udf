

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.hadoop.hive.ql.exec.UDF
import java.text.SimpleDateFormat
import java.util.Date

  class DateFormatCustom extends UDF{
    
    def evaluate(sdate :String):String={
      
      var resultFinal="none"
      
      if(sdate!=null && !sdate.isEmpty()){
      
        val simpleDate=new SimpleDateFormat("yyyy-mm-dd hh:mm:ss")
        
        val dDate=simpleDate.parse(sdate)
        
        val simpleDateFinal=new SimpleDateFormat("yyyy/MM/dd")
        
        resultFinal=simpleDateFinal.format(dDate)
      }
      
      resultFinal
      
    }
  }

object HiveUDFCustom {
  

  
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setMaster("local").setAppName("hive-udf")
    
    val sc=new SparkContext(conf)
    
    val sqlContext=new HiveContext(sc)
    
    
    sqlContext.sql("use default")
    
    val resultTables=sqlContext.sql("show tables")
    
    resultTables.collect().foreach { x => println(x) }
    
    sqlContext.sql("CREATE TEMPORARY FUNCTION date_format_custom AS 'DateFormatCustom' ")
    
    val result= sqlContext.sql("select "+ 
                          " id_social_account ,"+                 
                          " date_format_custom(creation_date)     ,"+  	
                          " date_format_custom(update_date)   ,"+     	              	                    
                          " name   ,"+             	              	                    
                          " lastname      ,"+      	              	                    
                          " password    ,"+        	              	                    
                          " email          "+     	 
        " from social_account limit 50 ")
    
    
    
    result.printSchema()
    
    result.collect().foreach { x => println(x) }
    
  }
}
