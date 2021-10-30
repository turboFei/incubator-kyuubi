/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.kyuubi.zorder

import org.apache.spark.sql.kyuubi.KyuubiSQLExtensionException

import java.lang.{Double => jDouble, Float => jFloat}
import org.apache.spark.sql.types.{BooleanType, ByteType, DataType, DateType, Decimal, DecimalType, DoubleType, FloatType, IntegerType, LongType, ShortType, StringType, TimestampType}
import org.apache.spark.unsafe.types.UTF8String

object ZorderBytesUtils {
  private final val BIT_8_MASK = 1 << 7
  private final val BIT_16_MASK = 1 << 15
  private final val BIT_32_MASK = 1 << 31
  private final val BIT_64_MASK = 1L << 63

  def interleaveMultiByteArray(arrays: Array[Array[Byte]]): Array[Byte] = {
    var totalLength = 0
    var maxLength = 0
    arrays.foreach(array => {
      totalLength += array.length
      maxLength = maxLength.max(array.length * 8)
    })
    val result = new Array[Byte](totalLength)
    var resultBit = 0

    var bit = 0
    while (bit < maxLength) {
      val bytePos = Math.floor(bit / 8).toInt
      val bitPos = bit % 8

      for (arr <- arrays) {
        if (bytePos < arr.length) {
          val resultBytePos = totalLength - 1 - Math.floor(resultBit / 8).toInt
          val resultBitPos = resultBit % 8
          result(resultBytePos) = updatePos(result(resultBytePos), resultBitPos,
            arr(arr.length - 1 - bytePos), bitPos)
          resultBit += 1
        }
      }
      bit += 1
    }
    result
  }

  def updatePos(a: Byte, apos: Int, b: Byte, bpos: Int): Byte = {
    var temp = (b & (1 << bpos)).toByte
    if (apos > bpos) {
      temp = (temp << (apos - bpos)).toByte
    } else if (apos < bpos) {
      temp = (temp >> (bpos - apos)).toByte
    }
    val atemp = (a & (1 << apos)).toByte
    if (atemp == temp) {
      return a
    }
    (a ^ (1 << apos)).toByte
  }

  def toByte(a: Any): Array[Byte] = {
    a match {
      case bo: Boolean =>
        booleanToByte(bo)
      case b: Byte =>
        byteToByte(b)
      case s: Short =>
        shortToByte(s)
      case i: Int =>
        intToByte(i)
      case l: Long =>
        longToByte(l)
      case f: Float =>
        floatToByte(f)
      case d: Double =>
        doubleToByte(d)
      case str: UTF8String =>
        // truncate or padding str to 8 byte
        paddingTo8Byte(str.getBytes)
      case dec: Decimal =>
        longToByte(dec.toLong)
      case other: Any =>
        throw new KyuubiSQLExtensionException("Unsupported z-order type: " + other.getClass)
    }
  }

  def booleanToByte(a: Boolean): Array[Byte] = {
    if (a) {
      byteToByte(1.toByte)
    } else {
      byteToByte(0.toByte)
    }
  }

  def byteToByte(a: Byte): Array[Byte] = {
    val tmp = (a ^ BIT_8_MASK).toByte
    Array(tmp)
  }

  def shortToByte(a: Short): Array[Byte] = {
    val tmp = a ^ BIT_16_MASK
    Array(((tmp >> 8) & 0xff).toByte, (tmp & 0xff).toByte)
  }

  def intToByte(a: Int): Array[Byte] = {
    val result = new Array[Byte](4)
    var i = 0
    val tmp = a ^ BIT_32_MASK
    while (i <= 3) {
      val offset = i * 8
      result(3 - i) = ((tmp >> offset) & 0xff).toByte
      i += 1
    }
    result
  }

  def longToByte(a: Long): Array[Byte] = {
    val result = new Array[Byte](8)
    var i = 0
    val tmp = a ^ BIT_64_MASK
    while (i <= 7) {
      val offset = i * 8
      result(7 - i) = ((tmp >> offset) & 0xff).toByte
      i += 1
    }
    result
  }

  def floatToByte(a: Float): Array[Byte] = {
    val fi = jFloat.floatToRawIntBits(a)
    intToByte(fi)
  }

  def doubleToByte(a: Double): Array[Byte] = {
    val dl = jDouble.doubleToRawLongBits(a)
    longToByte(dl)
  }

  def paddingTo8Byte(a: Array[Byte]): Array[Byte] = {
    val len = a.length
    if (len == 8) {
      a
    } else if (len > 8) {
      val result = new Array[Byte](8)
      System.arraycopy(a, 0, result, 0, 8)
      result
    } else {
      val result = new Array[Byte](8)
      System.arraycopy(a, 0, result, 8 - len, len)
      result
    }
  }

  def defaultValue(dataType: DataType): Array[Byte] = toByte {
    dataType match {
      case BooleanType =>
        false
      case ByteType =>
        Byte.MaxValue
      case ShortType =>
        Short.MaxValue
      case IntegerType | DateType =>
        Int.MaxValue
      case LongType | TimestampType | _: DecimalType =>
        Long.MaxValue
      case FloatType =>
        Float.MaxValue
      case DoubleType =>
        Double.MaxValue
      case StringType =>
        // we pad string to 8 bytes so it's equal to long
        UTF8String.fromBytes(longToByte(Long.MaxValue))
      case other: Any =>
        throw new KyuubiSQLExtensionException(s"Unsupported z-order type: ${other.catalogString}")
    }
  }
}
