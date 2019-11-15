package cse512

object HotzoneUtils {

  def ST_Contains(queryRectangle: String, pointString: String ): Boolean = {
    // YOU NEED TO CHANGE THIS PART
    //return true // YOU NEED TO CHANGE THIS PART

    //try {
    var _rectangle = new Array[String](4)
    _rectangle = queryRectangle.split(",")
    var rectx1 = _rectangle(0).trim.toDouble
    var recty1 = _rectangle(1).trim.toDouble
    var rectx2 = _rectangle(2).trim.toDouble
    var recty2 = _rectangle(3).trim.toDouble

    var points = new Array[String](2)
    points= pointString.split(",")
    var pointx=points(0).trim.toDouble
    var pointy=points(1).trim.toDouble


    var lower_x =0.0
    var higher_x =0.0

    if (rectx1 < rectx2)
    {
      lower_x = rectx1
      higher_x = rectx2
    }
    else
    {
      lower_x = rectx2
      higher_x = rectx1
    }

    var lower_y = math.min(recty1, recty2)
    var higher_y = math.max(recty1, recty2)

    if(pointy > higher_y || pointx < lower_x || pointx > higher_x || pointy < lower_y)
      return false
    else
      return true
    // }
    // catch {
    //     case _: Throwable => return false
    // }
  }
  // YOU NEED TO CHANGE THIS PART
}