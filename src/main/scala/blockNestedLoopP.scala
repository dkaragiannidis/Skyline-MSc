import org.apache.log4j.{Level, Logger}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.util.LongAccumulator
import scala.collection.mutable.ArrayBuffer
object blockNestedLoopP{
  def main(args: Array[String]): Unit = {
    var cores="local["+args(0)+"]"
    var partition=args(0).toInt
//var cores="local[*]"
//    var partition=6
    print("ekane to word count")
    Logger.getLogger("org").setLevel(Level.ERROR)
    val ss = SparkSession.builder().master(cores).appName("ask").getOrCreate()
    ss.sparkContext.setLogLevel("ERROR")
    import ss.implicits._
    val inputFile = "1000000anticorrelated.csv"
    val data = ss.read.text(inputFile).as[String]
//    val dfs = inputFile.flatMap(line => line.toString.split(" "))
    val words = data.flatMap(value => value.split("\\s+"))
    val groupedWords = words.groupByKey(_.toLowerCase)
    val df = groupedWords.count()
val countdf=df.count().toInt
//println("df",countdf)
    var basicDf = ss.read
      .option("header", "false")
      .csv(inputFile)

    print("pira ta arxeia")
    basicDf.printSchema()
    basicDf.show()
    val count = basicDf.count()
    print("count", count)
    var dimensions = countdf / count.toInt

    print("dimensions", dimensions)
    basicDf = basicDf.withColumn("temp", split(col("_c0"), "\\ ")).select(
      (0 until dimensions).map(i => col("temp").getItem(i).as(s"col$i").cast(DoubleType)): _*)
    println("meta  ton xwrismo")
    basicDf.printSchema()
    basicDf.show()
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


    timebasicdf(Skyline(basicDf,dimensions))
    //    println("ksekinaei o sfsssssssssssssssssssssssssssssssssssssssssssssssssssssssss")
    //  timebasicdf(Skyline(SfsData,dimensions.toInt))
    def Skyline(basicDfs: DataFrame, dimensions: Int) = {
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
      var record=0
      var totalrecord=basicDfs.count().toInt
      //var lastrow=basicDfs.rdd.takeOrdered(1)(Ordering[Row].reverse)
      //      var bnlBuffferLength = bnlBuffer.length - 1
      val accUnnamed = new LongAccumulator
      val acc = ss.sparkContext.register(accUnnamed)
      def accChinaFunc(flight_row:Broadcast[ArrayBuffer[Row]],currentRow:Row): Broadcast[ArrayBuffer[Row]] = {
//        println("print apo to destroy")
        //        broadcastBuffer.destroy()

        //        if(flight_row.value.length>0) {
        //  flight_row.value.foreach(println)
//        println(" mapneis reeeeeeeeeeee")

//        println("oel")

        broadcastBuffer = flight_row

        return broadcastBuffer

      }
//      cores.toInt
      val Apo=basicDfs.rdd.repartition(partition).map(x=>x)

        Apo.foreach(x=>
        if(broadcastBuffer.value.length==0){
          broadcastBuffer.value+=x
          broadcastBuffer=accChinaFunc(broadcastBuffer,x)
//          broadcastBuffer.value.foreach(println)
          record+=1
        }else{
          record+=1
          println("brisketai sthn egrafh" ,record)
//          println("to epomeno point einai:",x)
          dominate=0
          dominated=0
          noCompared=0
          var bnl=0
          var point=x
          var neuralEqual=0
//          println("gia to anathema",broadcastBuffer.value.length)
//          println("pws einai to dominate",dominate)
//          println("pws einai to bnl",bnl)
          var minus=0
          while((bnl<=broadcastBuffer.value.length-1)&&dominate==0){
//            println("mpainei st while",broadcastBuffer.value(bnl))
            trueCounter=0
            falseCounter=0
            neuralEqual=0

            for(d<-0 to dimensions.toInt-1 ){
//              println("mpainei sto for")
              broadcastBuffer=accChinaFunc(broadcastBuffer,x)
              //to shmeio toy pinaka kanei dominate to shmeio tou dataframe
              if (broadcastBuffer.value(bnl).getDouble(d)<point.getDouble(d)){

                trueCounter+=1
//                println("to trueCounter",trueCounter)
                if(trueCounter==dimensions.toInt ||trueCounter+neuralEqual==dimensions.toInt){
//                  println("petaei to simeio tou dataframe",x)
                  dominate=1
                }
              }
              else if(broadcastBuffer.value(bnl).getDouble(d)>point.getDouble(d)){

                falseCounter+=1
                if(dominate==0 &&((neuralEqual!=0 && falseCounter+neuralEqual==dimensions.toInt)||(falseCounter==dimensions.toInt))) {
//                  println("nbl",bnl)
                  broadcastBuffer.value -= broadcastBuffer.value(bnl)
                  broadcastBuffer = accChinaFunc(broadcastBuffer, point)
                  minus += 1
                  //                println("auto pu tha bgei",broadcastBuffer.value(bnl))
//                  println("upochfio gia na mpei",dominated)
//                  println("upochfio gia bufidia na mpei",broadcastBuffer.value.length)
                }
//                println("to falseCounter",falseCounter)
              }else if(broadcastBuffer.value(bnl).getDouble(d)==point.getDouble(d)){
                neuralEqual+=1
              }

              if(dominate==0 &&((neuralEqual!=0 && falseCounter+neuralEqual==dimensions.toInt)||(falseCounter==dimensions.toInt))){
//                println("upochfio gia na mpei",x)


                dominated+=1

              }
              else if(dominate==0 &&(trueCounter!=0 &&falseCounter!=0 &&(falseCounter+trueCounter+neuralEqual==dimensions.toInt||falseCounter+trueCounter==dimensions.toInt))){
//                println("upochfio gia noncompared na mpei",x)
                noCompared+=1
//                println("upochfio gia noncompared na mpei",noCompared)
//                println("upochfio gia noncomparedbuffer na mpei",broadcastBuffer.value.length)
              }
            }

            if (minus != 0) {
              if (minus > bnl) {

                bnl = 0
                minus=0
              }
              else   if (minus < bnl) {
                bnl = (bnl + 1) - minus
                minus=0
              }
            } else {

              bnl += 1

            }
//            println("bnl poso einai", bnl)
          }
//          println("to noncompared einai",noCompared)
//          println("to broadcastBuffer.value.length-1",broadcastBuffer.value.length-1)
//          broadcastBuffer.value.foreach(println)
          if(dominate==0 &&dominated==broadcastBuffer.value.length){
//            println("gioyxou mapeinei")
            broadcastBuffer.value+=point
            broadcastBuffer=accChinaFunc(broadcastBuffer,point)
          }
          else if(dominate==0 && noCompared==broadcastBuffer.value.length){

//            println("den sigkrinetai")
            broadcastBuffer.value+=point
            broadcastBuffer=accChinaFunc(broadcastBuffer,point)
          }
          else if(dominate==0 &&dominated!=0&& noCompared!=0 && noCompared+dominated==broadcastBuffer.value.length){
            broadcastBuffer.value+=point
            broadcastBuffer=accChinaFunc(broadcastBuffer,point)
          }
//          println("ante kala", broadcastBuffer.value.length)
          if(record==totalrecord){
            broadcastBuffer.value.foreach(println)
          }


        }

      )
      print(Apo)


    }

  }
}
