import org.apache.log4j.{Level, Logger}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.util.LongAccumulator

import scala.annotation.switch
import scala.collection.mutable.ArrayBuffer
object blockNestedLoopP{
  def main(args: Array[String]): Unit = {
        print("ekane to word count")
       var cores="local["+args(0)+"]"
        var partition=args(0).toInt
        var i= args(1).toInt
//    var cores="local[*]"
//    var i=1
//    var partition=6
    Logger.getLogger("org").setLevel(Level.ERROR)
    val ss = SparkSession.builder().master(cores).appName("ask").getOrCreate()
    ss.sparkContext.setLogLevel("ERROR")
    import ss.implicits._
    val inputFile =(i: @switch)  match {
      case 1=> "δατα.csv"
      case 2=> "1000000anticorrelated.csv"
      case 3=>"anticorreleated55000.csv"
      case 4=>"uniform1000000d10.csv"
    }
    val data = ss.read.text(inputFile).as[String]
    val dfs = inputFile.flatMap(line => line.toString.split(" "))
    val words = data.flatMap(value => value.split("\\s+"))
    val groupedWords = words.groupByKey(_.toLowerCase)
    val df = groupedWords.count()

    var basicDf = ss.read
      .option("header", "false")
      .csv(inputFile)

    print("pira ta arxeia")
    basicDf.printSchema()
    basicDf.show()
    val count = basicDf.count()
    print("count", count)
    var dimensions = ((df.count() - 1) / count) + 1
    print("dimensions", dimensions)
    basicDf = basicDf.withColumn("temp", split(col("_c0"), "\\ ")).select(
      (0 until dimensions.toInt).map(i => col("temp").getItem(i).as(s"col$i").cast(DoubleType)): _*)
    println("meta  ton xwrismo")
    basicDf.printSchema()
    basicDf.show()


    println("ksekinaei o bnl")
    timebasicdf(Skyline(basicDf,dimensions.toInt))
    def Skyline( basicDfs: DataFrame, dimensions: Int) = {
      var basicDfs2=basicDfs
      var start = true
      var bnlBuffer: ArrayBuffer[Row] = ArrayBuffer()
      var broadcastBuffer=ss.sparkContext.broadcast(bnlBuffer)
      var temporaryFiles1: ArrayBuffer[Row] = ArrayBuffer()
      var bufferDrive: ArrayBuffer[Row] = ArrayBuffer()
      var data: ArrayBuffer[Row] = ArrayBuffer()
      var trueCounter = 0
      var falseCounter = 0
      var dominate = 0
      var dominated = 0
      var noCompared = 0
      var morethanonep = 0
      var morethanoner = 0
      //var lastrow=basicDfs.rdd.takeOrdered(1)(Ordering[Row].reverse)
      //      var bnlBuffferLength = bnlBuffer.length - 1
      val accUnnamed = new LongAccumulator
      val acc = ss.sparkContext.register(accUnnamed)
      def accChinaFunc(flight_row:Broadcast[ArrayBuffer[Row]],currentRow:Row): Broadcast[ArrayBuffer[Row]] = {
        println("print apo to destroy")
        //        broadcastBuffer.destroy()

        //        if(flight_row.value.length>0) {
        //  flight_row.value.foreach(println)
        println(" mapneis reeeeeeeeeeee")

        println("oel")

        broadcastBuffer = flight_row

        return broadcastBuffer

      }

      basicDfs2=basicDfs2.repartition(partition)
      basicDfs2.count()
      println( "",basicDfs2.rdd.getNumPartitions)
      val da=basicDfs2.rdd.map(x=> {
        if (broadcastBuffer.value.length == 0) {
          broadcastBuffer.value += x
          broadcastBuffer = accChinaFunc(broadcastBuffer, x)
          broadcastBuffer.value.size
        } else {
          println("to epomeno point einai:", x)
          dominate = 0
          dominated = 0
          noCompared = 0
          var bnl = 0
          var point = x
          var neuralEqual = 0
          println("gia to anathema", broadcastBuffer.value.length)
          println("pws einai to dominate", dominate)
          println("pws einai to bnl", bnl)

          var minus = 0
          while ((bnl <= broadcastBuffer.value.length - 1) && dominate == 0) {
            println("mpainei st while", broadcastBuffer.value.count(x=>x!=x))
            trueCounter = 0
            falseCounter = 0
            neuralEqual = 0

            for (d <- 0 to dimensions.toInt - 1) {
              println("mpainei sto for")
              broadcastBuffer = accChinaFunc(broadcastBuffer, x)
              broadcastBuffer.value.count(x=>x!=x)
              //to shmeio toy pinaka kanei dominate to shmeio tou dataframe
              if (broadcastBuffer.value(bnl).getDouble(d) < point.getDouble(d)) {

                trueCounter += 1
                println("to trueCounter", trueCounter)
                if (trueCounter == dimensions.toInt || trueCounter + neuralEqual == dimensions.toInt) {
                  println("petaei to simeio tou dataframe", x)
                  dominate = 1
                }
              }
              else if (broadcastBuffer.value(bnl).getDouble(d) > point.getDouble(d)) {

                falseCounter += 1
                if (dominate == 0 && ((neuralEqual != 0 && falseCounter + neuralEqual == dimensions.toInt) || (falseCounter == dimensions.toInt))) {
                  println("nbl", bnl)
                  broadcastBuffer.value -= broadcastBuffer.value(bnl)
                  broadcastBuffer = accChinaFunc(broadcastBuffer, point)
                  //                    broadcastBuffer.value.count(x=>x!=x)
                  minus += 1

                  println("upochfio gia na mpei", dominated)
                  println("upochfio gia bufidia na mpei", broadcastBuffer.value.length)
                }
                //                  println("to falseCounter", falseCounter)

              } else if (broadcastBuffer.value(bnl).getDouble(d) == point.getDouble(d)) {
                neuralEqual += 1
              }

              if (dominate == 0 && ((neuralEqual != 0 && falseCounter + neuralEqual == dimensions.toInt) || (falseCounter == dimensions.toInt))) {
                println("upochfio gia na mpei", x)


                dominated += 1

              }
              else if (dominate == 0 && (trueCounter != 0 && falseCounter != 0 && (falseCounter + trueCounter + neuralEqual == dimensions.toInt || falseCounter + trueCounter == dimensions.toInt))) {
                println("upochfio gia noncompared na mpei", x)
                noCompared += 1
                println("upochfio gia noncompared na mpei", noCompared)
                println("upochfio gia noncomparedbuffer na mpei", broadcastBuffer.value.length)
              }
            }

            if (minus != 0) {
              if (minus > bnl) {

                bnl = 0
                minus = 0
              }
              else if (minus < bnl) {
                bnl = (bnl + 1) - minus
                minus = 0
              }
            } else {

              bnl += 1

            }
            println("bnl poso einai", bnl)

          }
          if (dominate == 0 && dominated == broadcastBuffer.value.length) {
            println("gioyxou mapeinei")
            broadcastBuffer.value += point
            broadcastBuffer = accChinaFunc(broadcastBuffer, point)
            broadcastBuffer.value.count(x=>x!=x)
          }
          else if (dominate == 0 && noCompared == broadcastBuffer.value.length) {

            println("den sigkrinetai")
            broadcastBuffer.value += point
            broadcastBuffer = accChinaFunc(broadcastBuffer, point)
            broadcastBuffer.value.count(x=>x!=x)
          }
          else if (dominate == 0 && dominated != 0 && noCompared != 0 && noCompared + dominated == broadcastBuffer.value.length) {
            broadcastBuffer.value += point
            broadcastBuffer = accChinaFunc(broadcastBuffer, point)
            broadcastBuffer.value.count(x=>x!=x)
          }


          broadcastBuffer.value.distinct.foreach(println)
          println("ante kala", broadcastBuffer.value.distinct.length)

        }
      })
      da.count()

    }}
  var maxtimedf=0
  var totalTimebasicDf=0
  def timebasicdf[R](block: => R): R = {
    val t0 = System.nanoTime()
    val result = block    // call-by-name
    val t1 = System.nanoTime()
    if(maxtimedf<(t1 - t0)){
      maxtimedf=t1.toInt - t0.toInt
    }
    totalTimebasicDf=totalTimebasicDf+(t1.toInt - t0.toInt)
    println("Elapsed time basicdf: " + (t1 - t0)/100000000 + "ns")
    //    print("maxbasicdf:", maxtimedf)
    result
  }
}
