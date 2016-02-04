/*
 * Copyright (c) Microsoft. All rights reserved.
 * Licensed under the MIT license. See LICENSE file in the project root for full license information.
 */

package org.apache.spark.sql.api.csharp

import java.io.{ByteArrayOutputStream, DataOutputStream}

import org.apache.spark.SparkContext
import org.apache.spark.api.csharp.SerDe
import org.apache.spark.api.java.{JavaRDD, JavaSparkContext}
import org.apache.spark.api.python.SerDeUtil
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DataType, FloatType, StructType}
import org.apache.spark.sql._
import java.util.{ArrayList => JArrayList}

/**
 * Utility functions for DataFrame in SparkCLR
 * The implementation is mostly identical to the SQLUtils used by R
 * since CSharpSpark derives most of the design ideas and
 * implementation constructs from SparkR
 */
object SQLUtils {
  def createSQLContext(sc: SparkContext): SQLContext = {
    new SQLContext(sc)
  }

  def getJavaSparkContext(sqlCtx: SQLContext): JavaSparkContext = {
    new JavaSparkContext(sqlCtx.sparkContext)
  }

  def toSeq[T](arr: Array[T]): Seq[T] = {
    arr.toSeq
  }

  def getSQLDataType(dataType: String): DataType = {
    dataType match {
      case "byte" => org.apache.spark.sql.types.ByteType
      case "integer" => org.apache.spark.sql.types.IntegerType
      case "float" => org.apache.spark.sql.types.FloatType
      case "double" => org.apache.spark.sql.types.DoubleType
      case "numeric" => org.apache.spark.sql.types.DoubleType
      case "character" => org.apache.spark.sql.types.StringType
      case "string" => org.apache.spark.sql.types.StringType
      case "binary" => org.apache.spark.sql.types.BinaryType
      case "raw" => org.apache.spark.sql.types.BinaryType
      case "logical" => org.apache.spark.sql.types.BooleanType
      case "boolean" => org.apache.spark.sql.types.BooleanType
      case "timestamp" => org.apache.spark.sql.types.TimestampType
      case "date" => org.apache.spark.sql.types.DateType
      case _ => throw new IllegalArgumentException(s"Invaid type $dataType")
    }
  }

  def dfToRowRDD(df: DataFrame): RDD[Array[Byte]] = {
    df.map(r => rowToCSharpBytes(r))
  }

  private[this] def doConversion(data: Object, dataType: DataType): Object = {
    data match {
      case d: java.lang.Double if dataType == FloatType =>
        new java.lang.Float(d)
      case _ => data
    }
  }


  private[this] def rowToCSharpBytes(row: Row): Array[Byte] = {
    val bos = new ByteArrayOutputStream()
    val dos = new DataOutputStream(bos)

    SerDe.writeInt(dos, row.length)
    (0 until row.length).map { idx =>
      val obj: Object = row(idx).asInstanceOf[Object]
      SerDe.writeObject(dos, obj)
    }
    bos.toByteArray()
  }

  def dfToCols(df: DataFrame): Array[Array[Byte]] = {
    // localDF is Array[Row]
    val localDF = df.collect()
    val numCols = df.columns.length
    // dfCols is Array[Array[Any]]
    val dfCols = convertRowsToColumns(localDF, numCols)

    dfCols.map { col =>
      colToCSharpBytes(col)
    }
  }

  def convertRowsToColumns(localDF: Array[Row], numCols: Int): Array[Array[Any]] = {
    (0 until numCols).map { colIdx =>
      localDF.map { row =>
        row(colIdx)
      }
    }.toArray
  }

  def colToCSharpBytes(col: Array[Any]): Array[Byte] = {
    val numRows = col.length
    val bos = new ByteArrayOutputStream()
    val dos = new DataOutputStream(bos)

    SerDe.writeInt(dos, numRows)

    col.map { item =>
      val obj: Object = item.asInstanceOf[Object]
      SerDe.writeObject(dos, obj)
    }
    bos.toByteArray()
  }

  def saveMode(mode: String): SaveMode = {
    mode match {
      case "append" => SaveMode.Append
      case "overwrite" => SaveMode.Overwrite
      case "error" => SaveMode.ErrorIfExists
      case "ignore" => SaveMode.Ignore
    }
  }

  def loadDF(
              sqlContext: SQLContext,
              source: String,
              options: java.util.Map[String, String]): DataFrame = {
    sqlContext.read.format(source).options(options).load()
  }

  def loadDF(
              sqlContext: SQLContext,
              source: String,
              schema: StructType,
              options: java.util.Map[String, String]): DataFrame = {
    sqlContext.read.format(source).schema(schema).options(options).load()
  }

  def loadDF(
              sqlContext: SQLContext,
              source: String,
              schema: StructType): DataFrame = {
    sqlContext.read.format(source).schema(schema).load()
  }

  def loadTextFile(
                    sqlContext: SQLContext,
                    path: String,
                    hasHeader: java.lang.Boolean,
                    inferSchema: java.lang.Boolean) : DataFrame = {
    var dfReader = sqlContext.read.format("com.databricks.spark.csv")
    if (hasHeader)
    {
      dfReader = dfReader.option("header", "true")
    }
    if (inferSchema)
    {
      dfReader = dfReader.option("inferSchema", "true")
    }
      dfReader.load(path)
  }

  def loadTextFile(
      sqlContext: SQLContext,
      path: String,
      delimiter: String,
      schemaJson: String) : DataFrame = {
    val stringRdd = sqlContext.sparkContext.textFile(path)

    val schema = createSchema(schemaJson)

    val rowRdd = stringRdd.map{s =>
      val columns = s.split(delimiter)
      columns.length match {
        case 1 => RowFactory.create(columns(0))
        case 2 => RowFactory.create(columns(0), columns(1))
        case 3 => RowFactory.create(columns(0), columns(1), columns(2))
        case 4 => RowFactory.create(columns(0), columns(1), columns(2), columns(3))
        case 5 => RowFactory.create(columns(0), columns(1), columns(2), columns(3), columns(4))
        // scalastyle:off
        case 6 => RowFactory.create(columns(0), columns(1), columns(2), columns(3), columns(4), columns(5))
        case 7 => RowFactory.create(columns(0), columns(1), columns(2), columns(3), columns(4), columns(5), columns(6))
        case 8 => RowFactory.create(columns(0), columns(1), columns(2), columns(3), columns(4), columns(5), columns(6), columns(7))
        case 9 => RowFactory.create(columns(0), columns(1), columns(2), columns(3), columns(4), columns(5), columns(6), columns(7), columns(8))
        case 10 => RowFactory.create(columns(0), columns(1), columns(2), columns(3), columns(4), columns(5), columns(6), columns(7), columns(8), columns(9))
        case 11 => RowFactory.create(columns(0), columns(1), columns(2), columns(3), columns(4), columns(5), columns(6), columns(7), columns(8), columns(9), columns(10))
        case 12 => RowFactory.create(columns(0), columns(1), columns(2), columns(3), columns(4), columns(5), columns(6), columns(7), columns(8), columns(9), columns(10), columns(11))
        case 13 => RowFactory.create(columns(0), columns(1), columns(2), columns(3), columns(4), columns(5), columns(6), columns(7), columns(8), columns(9), columns(10), columns(11), columns(12))
        case 14 => RowFactory.create(columns(0), columns(1), columns(2), columns(3), columns(4), columns(5), columns(6), columns(7), columns(8), columns(9), columns(10), columns(11), columns(12), columns(13))
        case 15 => RowFactory.create(columns(0), columns(1), columns(2), columns(3), columns(4), columns(5), columns(6), columns(7), columns(8), columns(9), columns(10), columns(11), columns(12), columns(13), columns(14))
        case 16 => RowFactory.create(columns(0), columns(1), columns(2), columns(3), columns(4), columns(5), columns(6), columns(7), columns(8), columns(9), columns(10), columns(11), columns(12), columns(13), columns(14), columns(15))
        case 17 => RowFactory.create(columns(0), columns(1), columns(2), columns(3), columns(4), columns(5), columns(6), columns(7), columns(8), columns(9), columns(10), columns(11), columns(12), columns(13), columns(14), columns(15), columns(16))
        case 18 => RowFactory.create(columns(0), columns(1), columns(2), columns(3), columns(4), columns(5), columns(6), columns(7), columns(8), columns(9), columns(10), columns(11), columns(12), columns(13), columns(14), columns(15), columns(16), columns(17))
        case 19 => RowFactory.create(columns(0), columns(1), columns(2), columns(3), columns(4), columns(5), columns(6), columns(7), columns(8), columns(9), columns(10), columns(11), columns(12), columns(13), columns(14), columns(15), columns(16), columns(17), columns(18))
        case 20 => RowFactory.create(columns(0), columns(1), columns(2), columns(3), columns(4), columns(5), columns(6), columns(7), columns(8), columns(9), columns(10), columns(11), columns(12), columns(13), columns(14), columns(15), columns(16), columns(17), columns(18), columns(19))
        case 21 => RowFactory.create(columns(0), columns(1), columns(2), columns(3), columns(4), columns(5), columns(6), columns(7), columns(8), columns(9), columns(10), columns(11), columns(12), columns(13), columns(14), columns(15), columns(16), columns(17), columns(18), columns(19), columns(20))
        case 22 => RowFactory.create(columns(0), columns(1), columns(2), columns(3), columns(4), columns(5), columns(6), columns(7), columns(8), columns(9), columns(10), columns(11), columns(12), columns(13), columns(14), columns(15), columns(16), columns(17), columns(18), columns(19), columns(20), columns(21))
        case 23 => RowFactory.create(columns(0), columns(1), columns(2), columns(3), columns(4), columns(5), columns(6), columns(7), columns(8), columns(9), columns(10), columns(11), columns(12), columns(13), columns(14), columns(15), columns(16), columns(17), columns(18), columns(19), columns(20), columns(21), columns(22))
        case 24 => RowFactory.create(columns(0), columns(1), columns(2), columns(3), columns(4), columns(5), columns(6), columns(7), columns(8), columns(9), columns(10), columns(11), columns(12), columns(13), columns(14), columns(15), columns(16), columns(17), columns(18), columns(19), columns(20), columns(21), columns(22), columns(23))
        case 25 => RowFactory.create(columns(0), columns(1), columns(2), columns(3), columns(4), columns(5), columns(6), columns(7), columns(8), columns(9), columns(10), columns(11), columns(12), columns(13), columns(14), columns(15), columns(16), columns(17), columns(18), columns(19), columns(20), columns(21), columns(22), columns(23), columns(24))
        case 26 => RowFactory.create(columns(0), columns(1), columns(2), columns(3), columns(4), columns(5), columns(6), columns(7), columns(8), columns(9), columns(10), columns(11), columns(12), columns(13), columns(14), columns(15), columns(16), columns(17), columns(18), columns(19), columns(20), columns(21), columns(22), columns(23), columns(24), columns(25))
        case 27 => RowFactory.create(columns(0), columns(1), columns(2), columns(3), columns(4), columns(5), columns(6), columns(7), columns(8), columns(9), columns(10), columns(11), columns(12), columns(13), columns(14), columns(15), columns(16), columns(17), columns(18), columns(19), columns(20), columns(21), columns(22), columns(23), columns(24), columns(25), columns(26))
        case 28 => RowFactory.create(columns(0), columns(1), columns(2), columns(3), columns(4), columns(5), columns(6), columns(7), columns(8), columns(9), columns(10), columns(11), columns(12), columns(13), columns(14), columns(15), columns(16), columns(17), columns(18), columns(19), columns(20), columns(21), columns(22), columns(23), columns(24), columns(25), columns(26), columns(27))
        case 29 => RowFactory.create(columns(0), columns(1), columns(2), columns(3), columns(4), columns(5), columns(6), columns(7), columns(8), columns(9), columns(10), columns(11), columns(12), columns(13), columns(14), columns(15), columns(16), columns(17), columns(18), columns(19), columns(20), columns(21), columns(22), columns(23), columns(24), columns(25), columns(26), columns(27), columns(28))
        case 30 => RowFactory.create(columns(0), columns(1), columns(2), columns(3), columns(4), columns(5), columns(6), columns(7), columns(8), columns(9), columns(10), columns(11), columns(12), columns(13), columns(14), columns(15), columns(16), columns(17), columns(18), columns(19), columns(20), columns(21), columns(22), columns(23), columns(24), columns(25), columns(26), columns(27), columns(28), columns(29))
        case _ => throw new Exception("Text files with more than 30 columns currently not supported") //TODO - if requirement comes up, generate code for additional columns
        // scalastyle:on
      }
    }

    sqlContext.createDataFrame(rowRdd, schema)
  }

  def createSchema(schemaJson: String) : StructType = {
    DataType.fromJson(schemaJson).asInstanceOf[StructType]
  }

  def byteArrayRDDToAnyArrayRDD(jrdd: JavaRDD[Array[Byte]]) : RDD[Array[_ >: AnyRef]] = {
    // JavaRDD[Array[Byte]] -> JavaRDD[Any]
    val jrddAny = SerDeUtil.pythonToJava(jrdd, true)

    // JavaRDD[Any] -> RDD[Array[_]]
    jrddAny.rdd.map {
      case objs: JArrayList[_] =>
        objs.toArray
      case obj if obj.getClass.isArray =>
        obj.asInstanceOf[Array[_]].toArray
    }
  }
}
