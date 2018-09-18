package cse512

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object AreaHotnessAnalysis {

  Logger.getLogger("org.spark_project").setLevel(Level.WARN)
  Logger.getLogger("org.apache").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)
  Logger.getLogger("com").setLevel(Level.WARN)

  def runHotRange(spark: SparkSession, pointPath: String, rectanglePath: String): DataFrame = {

    var pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter",";").option("header","false").load(pointPath);
    pointDf.createOrReplaceTempView("point")

    // Parse point data formats
    spark.udf.register("trim",(string : String)=>(string.replace("(", "").replace(")", "")))
    pointDf = spark.sql("select trim(_c5) as _c5 from point")
    pointDf.createOrReplaceTempView("point")
    //pointDf.show();
    // Load rectangle data
    val rectangleDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(rectanglePath);
    rectangleDf.createOrReplaceTempView("rectangle")
    //rectangleDf.show();
    // Join two datasets
    spark.udf.register("ST_Contains",(queryRectangle:String, pointString:String)=>(WithInRangeZone.WithInRange(queryRectangle, pointString)))

    val joinDf = spark.sql("select rectangle._c0 as rectangle, point._c5 as point from rectangle,point where WithInRange(rectangle._c0,point._c5)").persist()
    val result_hot_zone_ordered = joinDf.orderBy("rectangle")
    result_hot_zone_ordered.createOrReplaceTempView("viewAfterWithinRange")
    val finalDFHotZone= spark.sql("select rectangle,count(point) from viewAfterWithinRange group by rectangle")


  //finalDFHotZone.show()
    return finalDFHotZone
  }

}
