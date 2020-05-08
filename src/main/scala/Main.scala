import org.apache.spark.SparkContext
import tools.{HSIInputFormat, Init, JTool}

object Main {
  def main(args: Array[String]): Unit = {
    if (args.length < 4) {
      val str =
        """
          |args(0): the numbers of random vector
          |args(1): imageid
          |args(2): baseapi
          |args(3): pre-number of endmembers
        """
      println(str)
      sys.exit(-1)
    }
    var t1 = System.currentTimeMillis()
    // Init
    var image = Init.fetchImage((args(1).toInt), args(2))
    var hdfsinfo = Init.fetchHDFSInfo(image.getHdfsid, args(2))

    // get random vector
    var randomVector = args(0)
    // create random vector as skewers
    val mat = JTool.createMat(randomVector.toInt, image.getBands.toInt)

    // init spark
    val home = System.getenv("SPARK_HOME") //获得环境变量

    val spark = new SparkContext("local[4]", "PPI", home)
  // rows, cols, bands, datatype, interleave skewers
    val bconf = spark.broadcast(image.getRows, image.getSamples, image.getBands, image.getDatatype.toShort, image.getInterleave.toLowerCase, mat)

    val file = spark.newAPIHadoopFile(hdfsinfo.getUrl.replaceAll("\"",""), classOf[HSIInputFormat], classOf[Integer], classOf[Array[Byte]])
    var t2 = System.currentTimeMillis()
    var sss= file.collect()
    println("read data finshed! cost about " + (t2 - t1) +" seconds")
    val hsidata = file.map(pair => {
      val datatype = bconf.value._4 match {
        case 2 => 2.toShort
        case 4 => 4.toShort
        case 12 => 2.toShort
        case _ => {
          println("不支持的datasize格式!");

          sys.exit(-1)
        }
      }
      val data = pair._2 // classOf[Array[Byte]]
      val len = data.length // size of this partition(Byte)
      val col = bconf.value._2
      val bands = bconf.value._3
      val key = pair._1 / ( bands * datatype)
      val pixel = len / (datatype * bands)
//      println("pixel: "+ pixel)
      val fdata = bconf.value._5 match {  //header.getInter.toLowerCase()
        case "bil" => JTool.BtoFBil(data, pixel, col.toShort, bands.toShort, datatype)
        case "bip" => JTool.BtoFBip(data, pixel, bands.toShort, datatype)
        case _ => {
          println("不支持的interleave格式!" )
            sys.exit(-1)
        }
      }
      println("base: " + key)
      (key, fdata) // base position
      }).cache()

    // count max & min
    val maxMin = hsidata.map(pair => {
      val maxMin = JTool.calcMaxMin(pair._2, bconf.value._6, pair._1)
      maxMin
    }).reduce((x, y) => {
      //计算最终的最大最小值
      val mM = JTool.calFinalMaxMin(x, y) // position
      mM
    })

    // count pixel index
    val index = JTool.countIndex(maxMin, image.getRows * image.getSamples)

    println("count pixel index ok!")
    // index -> pos  count -> number

    val bc = spark.broadcast(index)
    //for 循环中的 yield 会把当前的元素记下来，保存在集合中，循环结束后将返回该集合。

    // pure (pos, vector, number)
    val pure = hsidata.flatMap(pair => {    //dataR:RDD
      for(i <- 0 until pair._2.length)    //until: last
        yield (pair._1 + i, pair._2(i),  bc.value(pair._1 + i.toInt))   // convert (position, vector)
    }).filter(x => {                      // filter those == 0
      bc.value(x._1.toInt) > args(0).toInt / (args(3).toInt * 5.0)         //index(x._1.toInt) > 0
    }).collect()


    // filter those similar spectral
    var map = Map[Array[Float], Int]()
    pure.foreach( (x) => {
      map += (x._2 -> x._3)
    })
    val mapSortBig = map.toList.sortBy(-_._2)
    mapSortBig.foreach(
      x=>{
        x._1.foreach(
          a=> print(a + ",")
        )
        println()
      }
    )



//    var list = List[Array[Float], Int]()
//    pure.foreach( (x) => {
////      list += (x._2 , x._3)
//    })




//    pdata(1)._3 = 3






  }


}
