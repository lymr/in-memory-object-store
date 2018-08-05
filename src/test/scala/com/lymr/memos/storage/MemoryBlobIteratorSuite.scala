package com.lymr.memos.storage

import org.scalatest.FunSuite

class MemoryBlobIteratorSuite extends FunSuite {

  test("iterate over empty space returns empty iterator") {

    val space = Vector.empty[Array[Byte]]

    val iterator = new MemoryBlobIterator(space, size = 0, blockSize = 8, start = 0, n = 0)

    assert(!iterator.hasNext)
  }

  test("start position is negative number results with exception") {

    val space = InitializeSpace(size = 15, blockSize = 8)

    intercept[IllegalArgumentException] {
      new MemoryBlobIterator(space, size = 15, blockSize = 8, start = -2, n = 0)
    }
  }

  test("negative elements count results with exception") {

    val space = InitializeSpace(size = 15, blockSize = 8)

    intercept[IllegalArgumentException] {
      new MemoryBlobIterator(space, size = 15, blockSize = 8, start = 2, n = -3)
    }
  }

  test("iterate over complete space") {

    val space = InitializeSpace(size = 16, blockSize = 8, (i, j) => (i * 8 + j).toByte)

    val iterator = new MemoryBlobIterator(space, size = 16, blockSize = 8, start = 0, n = 16)

    assertResult(0 until 16)(iterator.toSeq)
  }

  test("start position plus count exceeds space length results with exception") {

    val space = InitializeSpace(size = 13, blockSize = 8, (i, j) => (i * 8 + j).toByte)

    intercept[IndexOutOfBoundsException] {
      new MemoryBlobIterator(space, size = 13, blockSize = 8, start = 0, n = 15)
    }
  }

  test("iterate over sparse at end space") {

    val space = InitializeSpace(size = 19, blockSize = 8, (i, j) => (i * 8 + j).toByte)

    val iterator = new MemoryBlobIterator(space, size = 19, blockSize = 8, start = 0, n = 19)

    assertResult(0 until 19)(iterator.toSeq)
  }

  test("take zero elements from position zero") {

    val space = InitializeSpace(size = 19, blockSize = 8, (i, j) => (i * 8 + j).toByte)

    val iterator = new MemoryBlobIterator(space, size = 19, blockSize = 8, start = 0, n = 0)

    assert(!iterator.hasNext)
  }

  test("take zero elements from middle position") {

    val space = InitializeSpace(size = 19, blockSize = 8, (i, j) => (i * 8 + j).toByte)

    val iterator = new MemoryBlobIterator(space, size = 19, blockSize = 8, start = 12, n = 0)

    assert(!iterator.hasNext)
  }

  test("take some elements from some position") {

    val space = InitializeSpace(size = 19, blockSize = 8, (i, j) => (i * 8 + j).toByte)

    val iterator = new MemoryBlobIterator(space, size = 19, blockSize = 8, start = 5, n = 10)

    assertResult(5 until 15)(iterator.toSeq)
  }

  test("iterate over block of size 1") {

    val space = InitializeSpace(size = 19, blockSize = 1, (i, j) => (i * 1 + j).toByte)

    val iterator = new MemoryBlobIterator(space, size = 19, blockSize = 1, start = 5, n = 10)

    assertResult(5 until 15)(iterator.toSeq)
  }

  private def InitializeSpace(size: Int, blockSize: Int, f: (Int, Int) => Byte = (_, _) => 0): Vector[Array[Byte]] = {
    Array.tabulate[Byte](calculateBlocksNumber(size, blockSize), blockSize)(f).toVector
  }

  private def calculateBlocksNumber(size: Int, blockSize: Int): Int = {
    (size / blockSize) + (if (size % blockSize == 0) 0 else 1)
  }
}
