import org.apache.spark
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object WordCount {
  def main(args: Array[String]): Unit = {
    //local模式
    val conf = new SparkConf().setAppName("WC").setMaster("local[2]")

    //集群模式
//    val conf = new SparkConf().setAppName("WC").setMaster("spark://192.168.1.111:7077")
//    conf.setJars(List("D:\EEEEEEEEEEEEEEEEEEEEEEEEEE\sparkcleansum\clean-sum-demo\target\clean-sum-demo-1.0-SNAPSHOT.jar"))


    val sc = SparkSession.builder().config(conf).getOrCreate().sparkContext
    val file = sc.textFile("D:\\\\EEEEEEEEEEEEEEEEEEEEEEEEEE\\\\111.txt")

    val wc = file.flatMap(line => line.split(" ")).map(word => (word, 1)).reduceByKey(_ + _)
    val res = wc.collect()
    for (r <- res) {
      print(r)
    }
  }

}
