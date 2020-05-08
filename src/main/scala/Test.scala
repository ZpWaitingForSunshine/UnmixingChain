import breeze.linalg.{*, DenseMatrix, DenseVector, inv, pinv, sum}
import org.apache.spark.SparkContext
import tools.Init

object Test {
  def main(args: Array[String]): Unit = {
    //    val home = System.getenv("SPARK_HOME") //获得环境变量
    //
    //    val spark = new SparkContext("local[4]", "PPI", home)
    //
    //
    //
    //    val bb = spark.parallelize(1 to 30, 5)
    //
    //     val tt = bb.mapPartitionsWithIndex((x,iter)=>{
    //       var i = 0
    //       var result = List[Int]()
    //       while(iter.hasNext){
    //         i += iter.next()
    //       }
    //       result.::(x + "|" + i).iterator
    //
    //
    //     }).collect()
    //    for(x <- tt){
    //      println(x)
    //    }
    val dm = DenseMatrix((1.0, 2.0, 3.0), (4.0, 5.0, 6.0))
    val ll = sum(dm(*, ::)).asDenseMatrix
    print(ll)
    val pp = sum(ll.t(*, ::))
//    val xx = DenseVector.horzcat(ll:_*)
    print(pp)
  }

//  val ll = sum(dm(::,*))

}
