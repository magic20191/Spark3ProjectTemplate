package com.kin

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.junit.Test

@Test
object TestmysqlSaveApp {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder()
      .appName("DataSourceAPIApp")
      .master("local[2]")
      .getOrCreate()
    //需要使用固定写法DefaultSource
    //Caused by: java.lang.ClassNotFoundException: com.post.spark.bigdata.text.DefaultSource
    val mydf = sparkSession.read
      //      .text("file:///Users/wn/ide/idea/src/main/scala/com/post/spark/bigdata/app/people")
      .format("com.kin.customDefine.sources.text")
      .option("path","file:///E:\\code_home\\Spark3ProjectTemplate\\test\\testData\\peolpe.txt").load()
    mydf.printSchema()

    import sparkSession.implicits._

    mydf.select('name).filter("name != 'zhangsan'").filter('id>=2).show()

    mydf.write
      .format("com.kin.customDefine.sink.mysql")
      .option("schema",mydf.schema.toString())
      .mode(SaveMode.Append)
      .save()


    sparkSession.stop()
  }


}
