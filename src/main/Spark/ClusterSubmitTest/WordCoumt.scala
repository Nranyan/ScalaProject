package ClusterSubmitTest

import org.apache.spark.{SparkConf, SparkContext}


object WordCount {
    def main(args: Array[String]) {
        val conf = new SparkConf().setAppName("mySpark")
                .setMaster("spark://192.168.199.125:7077")
              //  .setMaster("local")
                .setJars(Array("C:\\Users\\NARAYAN\\IdeaProjects\\ProjectV2\\out\\artifacts\\ProjectV2_jar\\ProjectV2.jar"))
                .set("spark.worker.timeout","60")

        val sc = new SparkContext(conf)

        val rdd = sc.parallelize(List(1, 2, 3, 4, 5, 6)).map(_ * 3)
        val mappedRDD = rdd.filter(_ > 10).collect()
        //对集合求和
        println(rdd.reduce(_ + _))
        //输出大于10的元素
        for (arg <- mappedRDD)
            print(arg + " ")
        println()
        println("math is work")

    }
}