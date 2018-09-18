package cse512

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Calendar

object HotcellUtils {
  val coordinateStep = 0.01

  def CalculateCoordinate(inputString: String, coordinateOffset: Int): Int =
  {
    // Configuration variable:
    // Coordinate step is the size of each cell on x and y
    var result = 0
    coordinateOffset match
    {
      case 0 => result = Math.floor((inputString.split(",")(0).replace("(","").toDouble/coordinateStep)).toInt
      case 1 => result = Math.floor(inputString.split(",")(1).replace(")","").toDouble/coordinateStep).toInt
      // We only consider the data from 2009 to 2012 inclusively, 4 years in total. Week 0 Day 0 is 2009-01-01
      case 2 => {
        val timestamp = HotcellUtils.timestampParser(inputString)
        result = HotcellUtils.dayOfMonth(timestamp) // Assume every month has 31 days
      }
    }
    return result
  }

  def timestampParser (timestampString: String): Timestamp =
  {
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss")
    val parsedDate = dateFormat.parse(timestampString)
    val timeStamp = new Timestamp(parsedDate.getTime)
    return timeStamp
  }

  def dayOfYear (timestamp: Timestamp): Int =
  {
    val calendar = Calendar.getInstance
    calendar.setTimeInMillis(timestamp.getTime)
    return calendar.get(Calendar.DAY_OF_YEAR)
  }

  def dayOfMonth (timestamp: Timestamp): Int =
  {
    val calendar = Calendar.getInstance
    calendar.setTimeInMillis(timestamp.getTime)
    return calendar.get(Calendar.DAY_OF_MONTH)
  }
  /*
  this function is used to calculate how many neghboring cells can each cell have
   */
 def numberOfNeighborsForEachCell(xCoord:Int,yCoord:Int,zCoord:Int,minCoordX:Int,maxCoordX:Int,minCoordY:Int,maxCoordY:Int,minCoordZ:Int,maxCoordZ:Int): Int ={
   var surfaceCountVal=0
   val returnValWhereCorner=8
   val returnValWhereSurface=12
   val returnValWhereEdges=18
      if(surfaceCountVal<0){
        val checkVal= 10
        val testXcoord=maxCoordX
        val testYCoord=maxCoordY
        val testZCoord=maxCoordZ

        if(testXcoord<xCoord){
          return -1

        }
        if(testXcoord<yCoord){
          return -1
        }
        if(testZCoord<zCoord){
          return -1
        }

      }
      val defaultReturnValue=28;
      if (xCoord == maxCoordX || xCoord == minCoordX) {
        surfaceCountVal =surfaceCountVal+1
      }

      if (yCoord == minCoordY || yCoord == maxCoordY) {
        surfaceCountVal =surfaceCountVal+1
      }

      if (zCoord == minCoordZ || zCoord == maxCoordZ) {
        surfaceCountVal =surfaceCountVal+1
      }
      //returns the corner case value
      if(surfaceCountVal==3){
        return returnValWhereCorner
      }
   //returns the surface count value
     if(surfaceCountVal==2){
       return returnValWhereSurface
     }
   //returns the edge case value
     if(surfaceCountVal==1){
       return returnValWhereEdges
     }
   //returns the default value
     if(surfaceCountVal==0){
       return 27
     }
     return defaultReturnValue
 }

/*
This funtion is used to calculate statistics based on the given formula
It takes as an input the mean, standard deviation, neighbor count, neighbor sum weight, num cells
 */
  def calculateStatistics(mean:Double, stdDev: Double, neighCount: Int, neighSumWeight: Int, numcells: Int): Double =
  {
    val numeratorGettisOrd=calculateGettisOrdNr(neighSumWeight.toDouble,mean,neighCount.toDouble)
    val denominatorGettisOrd=calculateGettisOrdDr(stdDev,numcells.toDouble,neighCount.toDouble)

    return (numeratorGettisOrd/denominatorGettisOrd).toDouble
  }
  def calculateGettisOrdNr(neighSumWeight: Double, mean: Double, neighCount: Double):Double={
    val meanMulCount= mean * neighCount
    return neighSumWeight - meanMulCount
  }
  def calculateGettisOrdDr(stdDev: Double, numcells: Double, neighCount: Double):Double={
    val neighCountSq=neighCount*neighCount
    val numCellMulNeigh=numcells* neighCount
    val nr=numCellMulNeigh-neighCountSq
    val dr=numcells-1.0
    val sqRootVal=Math.sqrt(nr/dr).toDouble
    return stdDev*sqRootVal
  }
  def withinCoordinateStep(firstValue:Int,secondValue:Int):Boolean={
    if(Math.abs(firstValue-secondValue)<=1){
      return true
    }
    return false
  }
}
