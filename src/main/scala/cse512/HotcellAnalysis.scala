package cse512

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions._

object HotcellAnalysis {
  Logger.getLogger("org.spark_project").setLevel(Level.WARN)
  Logger.getLogger("org.apache").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)
  Logger.getLogger("com").setLevel(Level.WARN)

  def runHotcellAnalysis(spark: SparkSession, pointPath: String): DataFrame =
  {
    // Load the original data from a data source
    var pickupInfo = spark.read.format("com.databricks.spark.csv").option("delimiter",";").option("header","false").load(pointPath);
    pickupInfo.createOrReplaceTempView("nyctaxitrips")
    pickupInfo.show()

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
    pickupInfo.createOrReplaceTempView("pickupInfoView")
    pickupInfo.show()

    // Define the min and max of x, y, z
    val minX = -74.50/HotcellUtils.coordinateStep
    val maxX = -73.70/HotcellUtils.coordinateStep
    val minY = 40.50/HotcellUtils.coordinateStep
    val maxY = 40.90/HotcellUtils.coordinateStep
    val minZ = 1
    val maxZ = 31
    val numCells = (maxX - minX + 1)*(maxY - minY + 1)*(maxZ - minZ + 1)

    pickupInfo = spark.sql("select x,y,z from pickupInfoView where x>= " + minX + " and x<= " + maxX + " and y>= " + minY + " and y<= " + maxY + " and z>= " + minZ + " and z<= " + maxZ + " order by z,y,x")
    pickupInfo.createOrReplaceTempView("selected_Cell_Vals")

    pickupInfo = spark.sql("select x, y, z, count(*) as hotCells from selected_Cell_Vals group by x, y, z order by z,y,x")
    pickupInfo.createOrReplaceTempView("selected_Cell_Hotness")

    val sum_Selected_Ccells = spark.sql("select sum(hotCells) as sumHotCells from selected_Cell_Hotness")
    sum_Selected_Ccells.createOrReplaceTempView("sum_Selected_Ccells")


    val mean = (sum_Selected_Ccells.first().getLong(0).toDouble / numCells.toDouble).toDouble


    spark.udf.register("squared", (inputX: Int) => (((inputX*inputX).toDouble)))

    val sum_Squares = spark.sql("select sum(squared(hotCells)) as sum_Squares from selected_Cell_Hotness")
    sum_Squares.createOrReplaceTempView("sum_Squares")


    val standardDeviation = scala.math.sqrt(((sum_Squares.first().getDouble(0).toDouble / numCells.toDouble) - (mean.toDouble * mean.toDouble))).toDouble


    spark.udf.register("adjacentCells", (inputX: Int, inputY: Int, inputZ: Int, minX: Int, maxX: Int, minY: Int, maxY: Int, minZ: Int, maxZ: Int) => ((HotcellUtils.calculateAdjacentCells(inputX, inputY, inputZ, minX, minY, minZ, maxX, maxY, maxZ))))

    val adjacentCells = spark.sql("select adjacentCells(sch_1.x, sch_1.y, sch_1.z, " + minX + "," + maxX + "," + minY + "," + maxY + "," + minZ + "," + maxZ + ") as adjacentCellCount,"
      + "sch_1.x as x, sch_1.y as y, sch_1.z as z, "
      + "sum(sch_2.hotCells) as sumHotCells "
      + "from selected_Cell_Hotness as sch_1, selected_Cell_Hotness as sch_2 "
      + "where (sch_2.x = sch_1.x+1 or sch_2.x = sch_1.x or sch_2.x = sch_1.x-1) "
      + "and (sch_2.y = sch_1.y+1 or sch_2.y = sch_1.y or sch_2.y = sch_1.y-1) "
      + "and (sch_2.z = sch_1.z+1 or sch_2.z = sch_1.z or sch_2.z = sch_1.z-1) "
      + "group by sch_1.z, sch_1.y, sch_1.x "
      + "order by sch_1.z, sch_1.y, sch_1.x")
    adjacentCells.createOrReplaceTempView("adjacentCells")


    spark.udf.register("zScore", (adjacentCellCount: Int, sumHotCells: Int, numCells: Int, x: Int, y: Int, z: Int, mean: Double, standardDeviation: Double) => ((HotcellUtils.calculateZScore(adjacentCellCount, sumHotCells, numCells, x, y, z, mean, standardDeviation))))

    pickupInfo = spark.sql("select zScore(adjacentCellCount, sumHotCells, "+ numCells + ", x, y, z," + mean + ", " + standardDeviation + ") as getisOrdStatistic, x, y, z from adjacentCells order by getisOrdStatistic desc");
    pickupInfo.createOrReplaceTempView("zScore")


    pickupInfo = spark.sql("select x, y, z from zScore")
    pickupInfo.createOrReplaceTempView("finalPickupInfo")


    return pickupInfo
  }
}