import java.io.{File, FileInputStream}
import java.util.Calendar

import org.apache.commons.io.IOUtils
import tools.JTool

import scala.collection.immutable.ListMap

object single {
  def main(args: Array[String]): Unit = {

//    val pixels = 350 * 350
    val bands = 156
    val pixels = 95 * 95
//    val data = new FileInputStream(new File("/home/hadoop/data/c350bip/c350bip"))
//    val dd = JTool.BtoFBip(IOUtils.toByteArray(data), 350 * 350, bands, 2)

    var starttime1 = Calendar.getInstance().getTimeInMillis.toInt



    val data = new FileInputStream(new File("/home/hadoop/data/c350bip/samson_2"))
    val dd = JTool.BtoFBip(IOUtils.toByteArray(data), pixels, 156, 2)
    var starttime2 = Calendar.getInstance().getTimeInMillis.toInt
    println("data read time:" + (starttime2 - starttime1))
    val mat = JTool.createMat(10000 , bands)
    val maxMin = JTool.calcMaxMin(dd, mat, 0)
    // count pixel index
    val index = JTool.countIndex(maxMin, pixels)

    var nums: Map[Int, Int] = Map()
    for( i <- 0 until index.length){
      if(index(i) > 12)
        nums += (i -> index(i))
    }
    val res = ListMap(nums.toSeq.sortBy(_._2).reverse:_*)
    val num = new StringBuffer("[")
    for(i <- 0 until res.size){
      num.append("{")
      for(j <- 0 until bands){
        num.append(dd(res.toSeq(i)._1)(j) + ",")
      }
      num.append("},\n")
    }
    num.setCharAt(num.length() - 1, ']')
    var endtime = Calendar.getInstance().getTimeInMillis.toInt
    println("calu time: "+ (endtime - starttime2))
    println("all time: "+ (endtime - starttime1))
    println(num)

  }
}
