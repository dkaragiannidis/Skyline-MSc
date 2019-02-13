import org.apache.log4j.{Level, Logger}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.util.LongAccumulator

import scala.collection.mutable.ArrayBuffer
object Start{
  def main(args: Array[String]): Unit = {
    print("ekane to word count")
    Logger.getLogger("org").setLevel(Level.ERROR)
    val ss = SparkSession.builder().master("local[*]").appName("ask").getOrCreate()
    ss.sparkContext.setLogLevel("ERROR")
    import ss.implicits._
    val inputFile = "/home/dks/Desktop/Karagiannidis/generator/correleated35000.csv"
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

    val sumAll = basicDf.columns.map{ case x if x != "ID" => col(x) }.reduce(_ + _)

    var   SfsData=basicDf.withColumn("TOTAL", sumAll)
    //    //    basicDf = basicDf.withColumn("TOTAL", basicDf.columns.map(c => col(c)).reduce((c1, c2) => c1 + c2))

    println("================meta apo to thn dhmioyrgia total")
    //        basicDf.printSchema()
    //        basicDf.show()
    SfsData = SfsData.withColumn("Entropy", log($"TOTAL" + 1)).drop("TOTAL").sort("Entropy")
    //    SfsData=SfsData.drop("Entropy")
    SfsData.createOrReplaceTempView("x")
    SfsData=SfsData.sqlContext.sql(s"select row_number() over (order by Entropy) as id,* from x")
    SfsData.createOrReplaceTempView("xxx")
    var valueData=SfsData.count()/10
    var eliminationFilterBuffer:ArrayBuffer[Row]=ArrayBuffer()
    var eliminationFilterSelect=SfsData.sqlContext.sql(s"select *  from xxx order by Entropy asc  limit 500")

    //    eliminationFilterSelect.createOrReplaceTempView("xx")
    //    eliminationFilterSelect=eliminationFilterSelect.drop("id")

    //    eliminationFilterSelect.createOrReplaceTempView("final")
    //    eliminationFilterSelect.show()
    //γεμίζω το εelimination buffer.

    val dr=eliminationFilterSelect.rdd.repartition(6).map(x=>x)
    dr.toLocalIterator.foreach(x=> eliminationFilterBuffer+=x)
    println("elsadasd",eliminationFilterBuffer.size)
    var broadcastElBuffer=ss.sparkContext.broadcast(eliminationFilterBuffer)


    val array: ArrayBuffer[Int] = ArrayBuffer()
    var broadcastElFilter=ss.sparkContext.broadcast(array)
    val function: (Int => Boolean) = (arg: Int) => !array.contains(arg)
    val udfFiltering = udf(function)
//  SfsData=SfsData.filter(udfFiltering(col("id")))

  SfsData.show()

println("ante bgale akri")

    SfsData=SfsData.repartition(6).filter(x=>cleanData(x))
    SfsData.show()
    SfsData=SfsData.drop("id").drop("Entropy")
    Skyline(SfsData,dimensions.toInt)
    def cleanData(row: Row):Boolean={

      val accUnnamed = new LongAccumulator
//                val acc = ss.sparkContext.register(accUnnamed)
      //      val function: (Int => Boolean) = (arg: Int) => Skata.contains(arg)
      //      val udfFiltering = udf(function)
                def accChinaFunc(flight_row:Broadcast[ArrayBuffer[Row]]): Broadcast[ArrayBuffer[Row]] = {
                  broadcastElBuffer = flight_row
                  return broadcastElBuffer
                }
      val accUnnamedfilter = new LongAccumulator
//      val accfilter = ss.sparkContext.register(accUnnamedfilter)
      def accChinaFunc2(flight_row2:Broadcast[ArrayBuffer[Int]]): Broadcast[ArrayBuffer[Int]] = {
        broadcastElFilter = flight_row2
        return broadcastElFilter
      }
  var    trueCounter=0
  var    falseCounter=0
  var    neuralEqual=0
    var  bnl=0
      var   dominate=0
      var dominated=0
      var noCompared=0

while(bnl<broadcastElBuffer.value.length) {
  println("broadcastElBuffer.value.length",broadcastElBuffer.value.length)
  println("bnl", bnl)
  trueCounter=0
  falseCounter=0
  neuralEqual=0
  var minus=0
  for (d <- 1 to dimensions.toInt) {
//    println("d",d)
//    println("eliminationFilterBuffer(bnl).getDouble(d)",broadcastElBuffer.value(bnl).getDouble(d))
//    println("row.getDouble(d)",row.getDouble(d))
    if( (broadcastElBuffer.value(bnl).getDouble(d)<row.getDouble(d))){

      trueCounter+=1
      if(trueCounter==dimensions.toInt ||trueCounter+neuralEqual==dimensions.toInt){
//        println("prepei na bgei")

        dominate=1
      }
    }
    else if(broadcastElBuffer.value(bnl).getDouble(d)>row.getDouble(d)){
      falseCounter+=1
      if(dominate==0 &&((neuralEqual!=0 && falseCounter+neuralEqual==dimensions.toInt)||(falseCounter==dimensions.toInt))) {
//        println("nbl",bnl)
        broadcastElBuffer.value -=broadcastElBuffer.value(bnl)
        broadcastElBuffer=accChinaFunc(broadcastElBuffer)
        minus += 1
        //                println("auto pu tha bgei",broadcastBuffer.value(bnl))
//        println("upochfio gia na mpei",dominated)
//        println("upochfio gia bufidia na mpei",broadcastElBuffer.value.length)
      }
    }

    else if(broadcastElBuffer.value(bnl).getDouble(d)==row.getDouble(d)){
      neuralEqual+=1
    }

    if(dominate==0 &&((neuralEqual!=0 && falseCounter+neuralEqual==dimensions.toInt)||(falseCounter==dimensions.toInt))){



      dominated+=1

    }
    else if(dominate==0 &&(trueCounter!=0 &&falseCounter!=0 &&(falseCounter+trueCounter+neuralEqual==dimensions.toInt||falseCounter+trueCounter==dimensions.toInt))){

      noCompared+=1
//      println("upochfio gia noncompared na mpei",noCompared)
//      println("upochfio gia noncomparedbuffer na mpei",broadcastElBuffer.value.length)
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

}
      if(dominate==0 &&dominated==broadcastElBuffer.value.length){
//        println("gioyxou mapeinei")
        broadcastElBuffer.value+=row
        broadcastElBuffer=accChinaFunc(broadcastElBuffer)

      }
      else if(dominate==0 && noCompared==broadcastElBuffer.value.length){

//        println("den sigkrinetai")
        broadcastElBuffer.value+=row
        broadcastElBuffer=accChinaFunc(broadcastElBuffer)

      }
      else if(dominate==0 &&dominated!=0&& noCompared!=0 && noCompared+dominated==broadcastElBuffer.value.length){
        eliminationFilterBuffer+=row
        broadcastElBuffer=accChinaFunc(broadcastElBuffer)

      }
      else if(dominate==1){
        broadcastElFilter.value+=row.getInt(0)
        broadcastElFilter=accChinaFunc2(broadcastElFilter)
//        print("ara",array.size)
      }
      println("ok as")
      print(array.size)
      array.foreach(println)
      val function: (Int => Boolean) = (arg: Int) => !broadcastElFilter.value.contains(arg)
      return  function(row.getInt(0))

    }


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
      //var lastrow=basicDfs.rdd.takeOrdered(1)(Ordering[Row].reverse)
      //      var bnlBuffferLength = bnlBuffer.length - 1
      val accUnnamed = new LongAccumulator
      val acc = ss.sparkContext.register(accUnnamed)
      def accChinaFunc(flight_row:Broadcast[ArrayBuffer[Row]],currentRow:Row): Broadcast[ArrayBuffer[Row]] = {
        println("print apo to destroy")
        //        broadcastBuffer.destroy()

        //        if(flight_row.value.length>0) {
        //  flight_row.value.foreach(println)
//        println(" mapneis reeeeeeeeeeee")

        println("oel")

        broadcastBuffer = flight_row

        return broadcastBuffer

      }

      val Apo=basicDfs.rdd.repartition(6).map(x=>x).foreach(x=>
        if(broadcastBuffer.value.length==0){
          broadcastBuffer.value+=x
          broadcastBuffer=accChinaFunc(broadcastBuffer,x)
          broadcastBuffer.value.foreach(println)
        }else{
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
            println("mpainei st while",broadcastBuffer.value(bnl))
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
                  println("upochfio gia na mpei",dominated)
                  println("upochfio gia bufidia na mpei",broadcastBuffer.value.length)
                }
                println("to falseCounter",falseCounter)
              }else if(broadcastBuffer.value(bnl).getDouble(d)==point.getDouble(d)){
                neuralEqual+=1
              }

              if(dominate==0 &&((neuralEqual!=0 && falseCounter+neuralEqual==dimensions.toInt)||(falseCounter==dimensions.toInt))){
                println("upochfio gia na mpei",x)


                dominated+=1

              }
              else if(dominate==0 &&(trueCounter!=0 &&falseCounter!=0 &&(falseCounter+trueCounter+neuralEqual==dimensions.toInt||falseCounter+trueCounter==dimensions.toInt))){
                println("upochfio gia noncompared na mpei",x)
                noCompared+=1
                println("upochfio gia noncompared na mpei",noCompared)
                println("upochfio gia noncomparedbuffer na mpei",broadcastBuffer.value.length)
              }
            }
            //          breakable{
            //                    if(dominate ==1)break
            //                    //          efCounter=eliminationFilterBufferLength
            //                  }
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
            println("bnl poso einai", bnl)
          }
          println("to noncompared einai",noCompared)
          println("to broadcastBuffer.value.length-1",broadcastBuffer.value.length-1)
          broadcastBuffer.value.foreach(println)
          if(dominate==0 &&dominated==broadcastBuffer.value.length){
            println("gioyxou mapeinei")
            broadcastBuffer.value+=point
            broadcastBuffer=accChinaFunc(broadcastBuffer,point)
          }
          else if(dominate==0 && noCompared==broadcastBuffer.value.length){

            println("den sigkrinetai")
            broadcastBuffer.value+=point
            broadcastBuffer=accChinaFunc(broadcastBuffer,point)
          }
          else if(dominate==0 &&dominated!=0&& noCompared!=0 && noCompared+dominated==broadcastBuffer.value.length){
            broadcastBuffer.value+=point
            broadcastBuffer=accChinaFunc(broadcastBuffer,point)
          }
          println("ante kala", broadcastBuffer.value.length)
          broadcastBuffer.value.foreach(println)

        }

      )
      print(Apo)


    }}

  println("ksekinaei o sfsssssssssssssssssssssssssssssssssssssssssssssssssssssssss")
  //    timebasicdf(Skyline(SfsData,dimensions.toInt))

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
