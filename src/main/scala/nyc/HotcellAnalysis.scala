package cse512

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions._

import scala.collection.mutable.ListBuffer

object HotcellAnalysis {
  Logger.getLogger("org.spark_project").setLevel(Level.WARN)
  Logger.getLogger("org.apache").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)
  Logger.getLogger("com").setLevel(Level.WARN)

  /*
  Checks if point is between minimum and maximum values of lat , long and time.
   */
  def checkValidRangeOfPoint(xPoint:Double,yPoint:Double,zPoint:Double,maximumXRange:Double,maximumYRange:Double,minimumXRange:Double,minimumYRange:Double,maximumZRange:Double,minimumZrange:Double): Boolean={
    if(xPoint<=maximumXRange && xPoint>=minimumXRange&& yPoint<=maximumYRange&& yPoint>=minimumYRange&&zPoint<=maximumZRange&&zPoint>=minimumZrange){
      return true
    }

    return false
  }

  def runHotcellAnalysis(spark: SparkSession, pointPath: String): DataFrame =
  {
    // Load the original data from a data source
    var pickupInfo = spark.read.format("com.databricks.spark.csv").option("delimiter",";").option("header","false").load(pointPath);
    pickupInfo.createOrReplaceTempView("nyctaxitrips")
    //pickupInfo.show()

    // Assign cell coordinates based on pickup points
    spark.udf.register("CalculateX",(pickupPoint: String)=>((
      HotcellUtils.CalculateCoordinate(pickupPoint, 0)
      )))
    spark.udf.register("CalculateY",(pickupPoint: String)=>((
      HotcellUtils.CalculateCoordinate(pickupPoint, 1)
      )))
    spark.udf.register("CalculateZ",(pickupTime: String)=>((
      HotcellUtils.CalculateCoordinate(pickupTime, 2)
      )))

    pickupInfo = spark.sql("select CalculateX(nyctaxitrips._c5),CalculateY(nyctaxitrips._c5), CalculateZ(nyctaxitrips._c1) from nyctaxitrips")
    var newCoordinateName = Seq("x", "y", "z")
    pickupInfo = pickupInfo.toDF(newCoordinateName:_*)
    //pickupInfo.show()



    // Define the min and max of x, y, z
    val minX = -74.50/HotcellUtils.coordinateStep
    val maxX = -73.70/HotcellUtils.coordinateStep
    val minY = 40.50/HotcellUtils.coordinateStep
    val maxY = 40.90/HotcellUtils.coordinateStep
    val minZ = 1
    val maxZ = 31
    val numCells = (maxX - minX + 1)*(maxY - minY + 1)*(maxZ - minZ + 1)



    pickupInfo.createOrReplaceTempView("inputPoints")



    spark.udf.register("checkPointValidity", (xPoint:Double,yPoint:Double,zPoint:Double,maximumXRange:Double,maximumYRange:Double,minimumXRange:Double,minimumYRange:Double,maximumZRange:Double,minimumZrange:Double)
    => (HotcellAnalysis.checkValidRangeOfPoint(xPoint,yPoint,zPoint,maximumXRange,maximumYRange,minimumXRange,minimumYRange,maximumZRange,minimumZrange)))

/*
DF to check point validity.
 */
    val validInputPoints = spark.sql("select x,y,z from inputPoints where checkPointValidity(x,y,z,"+maxX+","+maxY+","+minX+","+minY+","+maxZ+","+minZ+")").persist()
    //validInputPoints.show()
    validInputPoints.createOrReplaceTempView("validPointsTemporaryView")
  /*
  DF to get total count for each point at a particular day of the month
   */
    val groupedDfWithCount=spark.sql("select x,y,z,count(*) as countNum from validPointsTemporaryView where checkPointValidity(x,y,z,"+maxX+","+maxY+","+minX+","+minY+","+maxZ+","+minZ+") group by x,y,z").persist()

    groupedDfWithCount.createOrReplaceTempView("groupingWithCountTemporaryView")
    //groupedDfWithCount.show()
    /*
    calculates overall sum,sigma and sigma square for calculation of std . deviation.
     */
    val sumOverallPoints = spark.sql("select sum(countNum) as sigmaXPoint, sum(countNum*countNum) as xSquareOfPoints from groupingWithCountTemporaryView")
    sumOverallPoints.createOrReplaceTempView("sumOfPoints")
    //sumpoints.show()
    val sigmaXPoint = sumOverallPoints.first().getLong(0)
    val squareSumCount = sumOverallPoints.first().getLong(1)

    val mean =(sigmaXPoint.toDouble / numCells.toDouble).toDouble
    val stdDev = Math.sqrt((squareSumCount.toDouble /numCells.toDouble) - (mean*mean))
    //val checkdf=spark.sql("select "+mean+","+stdDev)
    //checkdf.show()


    //registers UDF for determining if other points are neighbor of a cell or not.
    spark.udf.register("WithinCoordinateStep",(firstValue:Int,secondValue:Int)=>(HotcellUtils.withinCoordinateStep(firstValue:Int,secondValue:Int)))
    spark.udf.register("NumberOfNeighborsForEachCell",(xCoord:Int,yCoord:Int,zCoord:Int,minCoordX:Int,maxCoordX:Int,minCoordY:Int,maxCoordY:Int,minCoordZ:Int,maxCoordZ:Int)=>HotcellUtils.numberOfNeighborsForEachCell(xCoord:Int,yCoord:Int,zCoord:Int,minCoordX:Int,maxCoordX:Int,minCoordY:Int,maxCoordY:Int,minCoordZ:Int,maxCoordZ:Int))
    //self join the DF with count so that we can calculate total sum weight of neighbors of each cell
    val eachNeighborsWeight=spark.sql("select self1.x,self1.y,self1.z,self2.countNum from groupingWithCountTemporaryView self1 cross join groupingWithCountTemporaryView self2 where (WithinCoordinateStep(self1.x,self2.x)) and (WithinCoordinateStep(self1.y,self2.y)) and (WithinCoordinateStep(self1.z,self2.z))").persist()
    //eachNeighborsWeight.show(50);
    eachNeighborsWeight.createOrReplaceTempView("WithNeighborsView")

    val eachNeighborsSumWeightWithCount=spark.sql("select x,y,z,NumberOfNeighborsForEachCell(x,y,z,"+minX+","+maxX+","+minY+","+maxY+","+minZ+","+maxZ+") as neighCount, sum (countNum) as totalWeight from WithNeighborsView group by x,y,z").persist()
    //eachNeighborsSumWeightWithCount.show(50)
    eachNeighborsSumWeightWithCount.createOrReplaceTempView("WithNeighborsViewFinal")
  //registering UDF for getting gettis ord statistics
    spark.udf.register("GetGettisOrdStats",(mean:Double, stdDev: Double, neighCount: Int, neighSumWeight: Int, numcells: Int)=>(HotcellUtils.calculateStatistics(mean:Double, stdDev: Double, neighCount: Int, neighSumWeight: Int, numcells: Int)))
  //DF with statistics
    val dfWithStats=spark.sql("select x,y,z,GetGettisOrdStats("+mean+","+stdDev+",neighCount,totalWeight,"+numCells+") as statistics from WithNeighborsViewFinal order by statistics desc").persist()
    //dfWithStats.show(50)
    dfWithStats.createOrReplaceTempView("viewWithStats")
    //ordering based on hotness of cell
    val finalDfWithResult=spark.sql("select x,y,z from viewWithStats order by statistics desc")
    //finalDfWithResult.show(50)
    //var mapWithCount= groupedDfghkWithCount.collect(test).map(eachRowWithVal=>(eachRowWithVal.getInt(0).toString+":"+eachRowWithVal.getInt(1)+":"+eachRowWithVal.getInt(2).toString,eachRowWithVal.getLong(3))).toMappper



    return finalDfWithResult

  }
}

