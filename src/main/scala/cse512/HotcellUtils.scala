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

  def calculateAdjacentCells(_inputX: Int, _inputY: Int, _inputZ: Int, _minX: Int, _maxX: Int, _minY: Int, _maxY: Int, _minZ: Int, _maxZ: Int): Int =
  {
    var count = 0

    if (_inputX == _minX || _inputX == _maxX) {
      count += 1
    }

    if (_inputY == _minY || _inputY == _maxY) {
      count += 1
    }

    if (_inputZ == _minZ || _inputZ == _maxZ) {
      count += 1
    }

    if (count == 1) {
      return 17;
    } else if (count == 2) {
      return 11;
    } else if (count == 3) {
      return 7;
    }

    return 26;
  }

  def calculateZScore(_adjacentCellCount: Int, _sumHotCells: Int, _numCells: Int, _x: Int, _y: Int, _z: Int, _mean: Double, _standardDeviation: Double): Double =
  {
    val _dividend = (_sumHotCells.toDouble - (_mean * _adjacentCellCount.toDouble))
    val _divisor = _standardDeviation * math.sqrt((((_numCells.toDouble * _adjacentCellCount.toDouble) - (_adjacentCellCount.toDouble * _adjacentCellCount.toDouble)) / (_numCells.toDouble - 1.0).toDouble).toDouble).toDouble

    return (_dividend / _divisor).toDouble
  }
}