import org.apache.log4j.{Level, Logger}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.{col, split, _}
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.util.LongAccumulator

import scala.annotation.switch
import scala.collection.mutable.ArrayBuffer
object Salsa {
  def main(args: Array[String]): Unit = {
    var cores="local["+args(0)+"]"
    var partition=args(0).toInt
    var i= args(1).toInt
    //        var efNumberRecord=args(2)
    //    var cores="local[*]"
    //    var i=18
    //    var partition=6
    //    var efNumberRecord=150
    Logger.getLogger("org").setLevel(Level.ERROR)
    val ss = SparkSession.builder().master(cores).appName("ask").getOrCreate()
    ss.sparkContext.setLogLevel("ERROR")
    import ss.implicits._
    val inputFile =(i: @switch)  match {
      case 1=> "uniform/1000/uniform31000.csv"
      case 2=> "uniform/1000/uniform51000.csv"
      case 3=>"uniform/1000/uniform81000.csv"
      case 4=>"uniform/2000/uniform32000.csv"
      case 5=>"uniform/2000/uniform52000.csv"
      case 6=> "uniform/2000/uniform82000.csv"
      case 7=>"uniform/5000/uniform35000.csv"
      case 8=>"uniform/5000/uniform55000.csv"
      case 9=>"uniform/5000/uniform85000.csv"
      case 10=>"anticorrelated/1000/anticorrelated31000.csv"
      case 11=>"anticorrelated/1000/anticorrelated51000.csv"
      case 12=>"anticorrelated/1000/anticorrelated81000.csv"
      case 13=>"anticorrelated/2000/anticorrelated32000.csv"
      case 14=>"anticorrelated/2000/anticorrelated52000.csv"
      case 15=>"anticorrelated/2000/anticorrelated82000.csv"
      case 16=>"anticorrelated/5000/anticorrelated35000.csv"
      case 17=>"anticorrelated/5000/anticorreleated55000.csv"
      case 18=>"anticorrelated/5000/anticorrelated85000.csv"
      case 19=>"correlated/1000/correlated31000.csv"
      case 20=>"correlated/1000/correlated51000.csv"
      case 21=>"correlated/1000/correlated81000.csv"
      case 22=>"correlated/2000/correlated32000.csv"
      case 23=>"correlated/2000/correlated52000.csv"
      case 24=>"correlated/2000/correlated82000.csv"
      case 25=>"correlated/5000/correlated35000.csv"
      case 26=>"correlated/5000/correlated55000.csv"
      case 27=>"correlated/5000/correlated85000.csv"
      case 28=>"gausian/1000/gauss31000.csv"
      case 29=>"gausian/1000/gauss51000.csv"
      case 30=>"gausian/1000/gauss81000.csv"
      case 31=>"gausian/2000/gauss32000.csv"
      case 32=>"gausian/2000/gauss52000.csv"
      case 33=>"gausian/2000/gauss82000.csv"
      case 34=>"gausian/5000/gauss35000.csv"
      case 35=>"gausian/5000/gauss55000.csv"
      case 36=>"gausian/5000/gauss85000.csv"
      case 37=>"anticorrelated/100000/anticorrelated3100000.csv"
      case 38=>"anticorrelated/100000/anticorrelated5100000.csv"
      case 39=>"anticorrelated/100000/anticorrelated8100000.csv"
      case 40=>"anticorrelated/1000000/anticorrelated31000000.csv"
      case 41=>"anticorrelated/1000000/anticorrelated51000000.csv"
      case 42=>"anticorrelated/1000000/anticorrelated81000000.csv"
      case 43=>"correlated/100000/correlated3100000.csv"
      case 44=>"correlated/100000/correlated5100000.csv"
      case 45=>"correlated/100000/correlated8100000.csv"
      case 46=>"correlated/1000000/correlated31000000.csv"
      case 47=>"correlated/1000000/correlated51000000.csv"
      case 48=>"correlated/1000000/correlated81000000.csv"
      case 49=>"uniform/100000/uniform3100000.csv"
      case 50=>"uniform/100000/uniform5100000.csv"
      case 51=>"uniform/100000/uniform8100000.csv"
      case 52=>"uniform/1000000/uniform31000000.csv"
      case 53=>"uniform/1000000/uniform51000000.csv"
      case 54=>"uniform/1000000/uniform81000000.csv"
      case 55=>"gausian/100000/gauss3100000.csv"
      case 56=>"gausian/100000/gauss5100000.csv"
      case 57=>"gausian/100000/gauss8100000.csv"
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
    var   columnsName:ArrayBuffer[String]=ArrayBuffer()


    var dataframeMax = 0.0
    for (i <- 0 to dimensions.toInt-1) {
      val ColumnMax = basicDf.agg(max(s"col$i")).head()
      val maxs = ColumnMax.getDouble(0)
      if (dataframeMax < maxs) {
        dataframeMax = maxs
      }
      print(dataframeMax)

    }
    var dataframeMin = 0.0
    for (i <- 0 to dimensions.toInt-1) {
      val ColumnMax = basicDf.agg(min(s"col$i")).head()
      val mins = ColumnMax.getDouble(0)
      if (dataframeMin > mins) {
        dataframeMin = mins
      }
      print(dataframeMax)

    }
    //    for (i <- 0 to dimensions.toInt - 1) {
    //                basicDf = basicDf.withColumn(s"names$i", (col(s"col$i")-dataframeMin) / (dataframeMax-dataframeMin)).drop(s"col$i")
    //              }
    for(d<-0 until dimensions.toInt){
      columnsName+=s"col$d"
      println(" cols.size", columnsName.size)
    }
    val sumAll = basicDf.columns.map{ case x if x != "ID" => col(x) }.reduce(_ + _)

    basicDf=basicDf.withColumn("Sum", sumAll)
    basicDf.show()
    basicDf=basicDf.withColumn("fmin",least(columnsName.head,columnsName.tail:_*))
    //    basicDf=basicDf.withColumn("maxperrow",)
    basicDf.show()
    basicDf=basicDf.sort("fmin","Sum")
    basicDf.show()
    //    basicDf.createOrReplaceTempView("x")
    //
    //    basicDf =basicDf.sqlContext.sql(s"select row_number() over (order by fmin) as id,* from x")
    basicDf.show()
    var maxrow=0.0

    timebasicdf(Skyline(basicDf,dimensions.toInt))

    def Skyline( basicDfs: DataFrame, dimensions: Int) = {
      var pStop:ArrayBuffer[Double]=ArrayBuffer()
      var broadcastPstop=ss.sparkContext.broadcast(pStop)
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

        println(" mapneis reeeeeeeeeeee")

        println("oel")

        broadcastBuffer = flight_row


        return broadcastBuffer

      }
      val accuml = new LongAccumulator
      //      val accss = ss.sparkContext.register(accuml)
      def pstopBroad(pstopValue:Broadcast[ArrayBuffer[Double]]):Broadcast[ArrayBuffer[Double]]={

        broadcastPstop=pstopValue

        return broadcastPstop
      }
      def getMax(x:Row): Double ={
        var  max=0.0
        for (d<-0 to dimensions.toInt-1){
          if(max<x.getDouble(d)){
            max=x.getDouble(d)
          }
        }
        return max
      }
      basicDfs2=basicDfs2.repartition(partition)
      basicDfs2.count()
      println( "",basicDfs2.rdd.getNumPartitions)
      val da=basicDfs2.rdd.map(x=> {
        if (broadcastBuffer.value.length == 0) {
          println("mapinei edwsd")
          broadcastBuffer.value += x
          broadcastBuffer = accChinaFunc(broadcastBuffer, x)
          broadcastBuffer.value.count(x=>x!=x)
          if(broadcastPstop.value.length==0){
            broadcastPstop.value+=getMax(x)
            broadcastPstop=pstopBroad(broadcastPstop)
            broadcastPstop.value.count(x=>x!=x)
          }else{
            broadcastPstop=pstopBroad(broadcastPstop)
            broadcastPstop.value.count(x=>x!=x)
          }
        } else {
          println("to epomeno point einai:", x)
          dominate = 0
          dominated = 0
          noCompared = 0
          var bnl = 0
          var point = x
          var neuralEqual = 0
          println("gia to anathema", broadcastBuffer.value.length)

          println("gia to anathema", broadcastPstop.value.length)
          println("pws einai to dominate", dominate)
          println("pws einai to bnl", bnl)

          var minus = 0
          broadcastPstop=pstopBroad(broadcastPstop)
          broadcastPstop.value.count(x=>x!=x)
          if (broadcastPstop.value(0) >= x.getDouble(dimensions.toInt + 1)){
            while ((bnl <= broadcastBuffer.value.length - 1) && dominate == 0) {

              trueCounter = 0
              falseCounter = 0
              neuralEqual = 0

              for (d <- 0 to dimensions.toInt - 1) {
                println("mpainei sto for")


                broadcastBuffer = accChinaFunc(broadcastBuffer, x)
                broadcastBuffer.value.count(x =>x!= x)
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
                    broadcastBuffer.value.count(x => x != x)
                    broadcastPstop=pstopBroad(broadcastPstop)
                    broadcastPstop.value.count(x=>x!=x)
                    minus += 1

                    println("upochfio gia na mpei", dominated)
                    println("upochfio gia bufidia na mpei", broadcastBuffer.value.length)
                  }
                  //                  println("to falseCounter", falseCounter)

                } else if (broadcastBuffer.value(bnl).getDouble(d) == point.getDouble(d)) {
                  neuralEqual += 1
                }

                if (dominate == 0 && ((neuralEqual != 0 && falseCounter + neuralEqual == dimensions.toInt) || (falseCounter == dimensions.toInt))) {

                  dominated += 1

                }
                else if (dominate == 0 && (trueCounter != 0 && falseCounter != 0 && (falseCounter + trueCounter + neuralEqual == dimensions.toInt || falseCounter + trueCounter == dimensions.toInt))) {
                  println("upochfio gia noncompared na mpei", x)
                  noCompared += 1
                  println("upochfio gia noncompared na mpei", noCompared)
                  println("upochfio gia noncomparedbuffer na mpei", broadcastBuffer.value.length)
                }
              }

              if (noCompared > broadcastBuffer.value.length) {
                noCompared = 0
                dominated = 0
                bnl = 0
              } else {
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
            }
            if (dominate == 0 && dominated == broadcastBuffer.value.length) {
              println("gioyxou mapeinei")
              broadcastBuffer.value += point
              broadcastBuffer = accChinaFunc(broadcastBuffer, point)
              broadcastBuffer.value.count(x => x != x)
              broadcastPstop.value+=getMax(point)
              broadcastPstop=pstopBroad(broadcastPstop)
              broadcastPstop.value.count(x=>x!=x)
            }
            else if (dominate == 0 && noCompared == broadcastBuffer.value.length) {

              println("den sigkrinetai")
              broadcastBuffer.value += point
              broadcastBuffer = accChinaFunc(broadcastBuffer, point)
              broadcastBuffer.value.count(x => x != x)
              broadcastPstop.value+=getMax(point)
              broadcastPstop=pstopBroad(broadcastPstop)
              broadcastPstop.value.count(x=>x!=x)
            }
            else if (dominate == 0 && dominated != 0 && noCompared != 0 && ((noCompared + dominated) == broadcastBuffer.value.length)) {
              broadcastBuffer.value += point
              broadcastBuffer = accChinaFunc(broadcastBuffer, point)
              broadcastBuffer.value.count(x => x != x)
              broadcastPstop.value+=getMax(point)
              broadcastPstop=pstopBroad(broadcastPstop)
              broadcastPstop.value.count(x=>x!=x)
            }
            broadcastBuffer.value.distinct.foreach(println)
            println("ante kala", broadcastBuffer.value.distinct.length)
            broadcastBuffer = accChinaFunc(broadcastBuffer, point)
            broadcastBuffer.value.count(x => x != x)
            broadcastPstop=pstopBroad(broadcastPstop)
            broadcastPstop.value.count(x=>x!=x)
          }else{
            broadcastBuffer.value.distinct.foreach(println)
            println("ante kala2", broadcastBuffer.value.distinct.length)
            broadcastBuffer = accChinaFunc(broadcastBuffer, point)
            broadcastBuffer.value.count(x=>x!=x)
            broadcastPstop=pstopBroad(broadcastPstop)
            broadcastPstop.value.count(x=>x!=x)

          }
        }
      })

      da.count()
      print("ok")
      da.foreach(println)

    }
    //    }


  }
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
    println("Elapsed time basicdf: " + (t1 - t0)/1000000000 + "ns")
    //    print("maxbasicdf:", maxtimedf)
    result
  }
}