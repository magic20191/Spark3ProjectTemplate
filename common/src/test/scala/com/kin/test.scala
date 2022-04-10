package com.kin

import com.kin.utils.DruidUtils

object test {
  def main(args: Array[String]): Unit = {

    val conn = DruidUtils.getConnection //从连接池中获取一个连接
    conn.setAutoCommit(false)
    val tableInfo = conn.getMetaData.getColumns(null, null, "product", null);
    var map:Map[String,String] = Map()
    while (tableInfo.next()) {
      map += (tableInfo.getString("COLUMN_NAME").toUpperCase() -> tableInfo.getString("TYPE_NAME"))
//      val TABLE_CAT = tableInfo.getString("TABLE_CAT");
//      val TABLE_SCHEM = tableInfo.getString("TABLE_SCHEM");
//      val TABLE_NAME = tableInfo.getString("TABLE_NAME");
//      val columnName = tableInfo.getString("COLUMN_NAME");
//      val typeName = tableInfo.getString("TYPE_NAME");
//      val columnSize = tableInfo.getString("COLUMN_SIZE");
//      val REMARKS = tableInfo.getString("REMARKS");
//      println(TABLE_CAT, TABLE_SCHEM, TABLE_NAME, columnName, typeName, columnSize, REMARKS);
//      (biyesheji,null,product,id,INT,10,)
//      (biyesheji,null,product,name,VARCHAR,50,商品名称)
//      (biyesheji,null,product,price,FLOAT,10,价格)
//      (biyesheji,null,product,zan,INT,10,赞)
    }
    map
                dateType match {
                  case _: ByteType => preparedStatement.setInt(i, record.getAs[Int](i - 1))
                  case _: ShortType => preparedStatement.setInt(i, record.getAs[Int](i - 1))
                  case _: IntegerType => preparedStatement.setInt(i, record.getAs[Int](i - 1))
                  case _: LongType => preparedStatement.setLong(i, record.getAs[Long](i - 1))
                  case _: BooleanType => preparedStatement.setInt(i, if (record.getAs[Boolean](i - 1)) 1 else 0)
                  case _: FloatType => preparedStatement.setFloat(i, record.getAs[Float](i - 1))
                  case _: DoubleType => preparedStatement.setDouble(i, record.getAs[Double](i - 1))
                  case _: StringType => preparedStatement.setString(i, record.getAs[String](i - 1))
                  case _: TimestampType => preparedStatement.setTimestamp(i, record.getAs[Timestamp](i - 1))
                  case _: DateType => preparedStatement.setDate(i, record.getAs[Date](i - 1))
                  case _ => throw new RuntimeException(s"nonsupport ${dateType} !!!")
                }

  }
}
