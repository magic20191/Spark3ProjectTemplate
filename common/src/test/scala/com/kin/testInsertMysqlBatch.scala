//package com.kin
//
//import java.sql.{Date, Timestamp}
//
//import com.kin.utils.DruidUtils
//import org.apache.spark.sql.types.{BooleanType, ByteType, DateType, DoubleType, FloatType, IntegerType, LongType, ShortType, StringType, TimestampType}
//import org.apache.spark.sql.{SaveMode, SparkSession}
//
//object TestmysqlSaveApp {
//
//
//  def main(args: Array[String]): Unit = {
//    val sparkSession = SparkSession.builder()
//      .appName("DataSourceAPIApp")
//      .master("local[2]")
//      .getOrCreate()
//    //需要使用固定写法DefaultSource
//    //Caused by: java.lang.ClassNotFoundException: com.post.spark.bigdata.text.DefaultSource
//    val mydf = sparkSession.read
//      //      .text("file:///Users/wn/ide/idea/src/main/scala/com/post/spark/bigdata/app/people")
//      .format("com.kin.customDefine.sources.text")
//      .option("path","file:///E:\\code_home\\Spark3ProjectTemplate\\test\\testData\\peolpe.txt").load()
//    mydf.printSchema()
//
//    import sparkSession.implicits._
//
//    val mydf2 = mydf.select('name)
//      .filter("name != 'zhangsan'")
//      .filter('id>=2)
//
//    mydf2.rdd.foreachPartition(partitionRecords => {
//     try {
//       val conn = DruidUtils.getConnection //从连接池中获取一个连接
//       conn.setAutoCommit(false)
//
//       var preparedStatement = conn.prepareStatement(
//         "insert into dup(id,name,phone ) values(?,?,?) where id<>?")
//
//       /*通过连接获取表名对应数据表的元数据*/
//       val metaData = conn.getMetaData.getColumns(null, "%", "tbname", "%")
//       var batchIndex = 0
//        partitionRecords.foreach(record => {  //装载数据
//
//          /*装载数据*/
//          ("")
//          if (value != null) { //如何值不为空,将类型转换为String
//            preparedStatement.setString(i, value.toString)
//            dateType match {
//              case _: ByteType => preparedStatement.setInt(i, record.getAs[Int](i - 1))
//              case _: ShortType => preparedStatement.setInt(i, record.getAs[Int](i - 1))
//              case _: IntegerType => preparedStatement.setInt(i, record.getAs[Int](i - 1))
//              case _: LongType => preparedStatement.setLong(i, record.getAs[Long](i - 1))
//              case _: BooleanType => preparedStatement.setInt(i, if (record.getAs[Boolean](i - 1)) 1 else 0)
//              case _: FloatType => preparedStatement.setFloat(i, record.getAs[Float](i - 1))
//              case _: DoubleType => preparedStatement.setDouble(i, record.getAs[Double](i - 1))
//              case _: StringType => preparedStatement.setString(i, record.getAs[String](i - 1))
//              case _: TimestampType => preparedStatement.setTimestamp(i, record.getAs[Timestamp](i - 1))
//              case _: DateType => preparedStatement.setDate(i, record.getAs[Date](i - 1))
//              case _ => throw new RuntimeException(s"nonsupport ${dateType} !!!")
//            }
//          } else { //如果值为空,将值设为对应类型的空值
//            metaData.absolute(i)
//            preparedStatement.setNull(i, metaData.getInt("DATA_TYPE"))
//          }
//
//
//          preparedStatement.addBatch()  // 加入批次
//          batchIndex +=1
//          // MySQL的批量写入尽量限制提交批次的数据量，否则会把MySQL写挂！！！
//          if(batchIndex % 2000 == 0 && batchIndex !=0){
//            preparedStatement.executeBatch()
//            preparedStatement.clearBatch()
//          }
//        })
//        preparedStatement.executeBatch()
//        conn.commit()
//      } catch {
//        case e: Exception => println(s"@Error@ insertOrUpdateDFtoDBUsePool ${e.getMessage}")
//        // do some log
//      } finally {
//        DruidUtils.close(preparedStatement,conn)
//      }
//
//
//
//    })
//
//
//    sparkSession.stop()
//
//
//  }
//
//}
