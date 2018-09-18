package cse512

object WithInRangeZone {
  /*
  Calculates whether a point belons within the rectangular pointset
   */
  def WithInRange(queryRectangle: String, pointString: String ): Boolean = {
    var arrayStringRectangle=queryRectangle.split(",")
    //convert string to double values
    var xCoordinate1=arrayStringRectangle(0).trim.toDouble
    var yCoordinate1=arrayStringRectangle(1).trim.toDouble
    var xCoordinate2=arrayStringRectangle(2).trim.toDouble
    var yCoordinate2=arrayStringRectangle(3).trim.toDouble

    var arrayPointString=pointString.split(",")
    var pointX=arrayPointString(0).trim.toDouble
    var pointY=arrayPointString(1).trim.toDouble
    //initialise variables
    var (xLowerDiagonal,xHigherDiagonal,yLowerDiagonal,yHigherDiagonal)= (0.0,0.0,0.0,0.0)
    //determine the lower and higher values
    /*
    if (xCoordinate1 < xCoordinate2)
    {
      xLowerDiagonal=xCoordinate1
      xHigherDiagonal=xCoordinate2
    } else{
      xLowerDiagonal=xCoordinate2
      xHigherDiagonal=xCoordinate1
    }
    if (yCoordinate1 < yCoordinate2){
      yLowerDiagonal=yCoordinate1
      yHigherDiagonal=yCoordinate2
    } else{
      yLowerDiagonal=yCoordinate2
      yHigherDiagonal=yCoordinate1
    }*/
    //check whther point is within the lower and higher values
    if(pointX<=xHigherDiagonal && pointX>=xLowerDiagonal && pointY<=yHigherDiagonal && pointY>=yLowerDiagonal){
      return true
    }else{
      return false
    }
  }


}
