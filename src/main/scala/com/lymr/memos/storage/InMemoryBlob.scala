package com.lymr.memos.storage

import com.lymr.memos.storage.InMemoryBlob._

import scala.annotation.tailrec

/** An in memory space represented by a `Vector` of separate byte array blocks where each byte
  * array block has a fixed size.
  *
  * ===A constant width block has the advantages of:===
  *  - Amortized performance of sequential memory read.
  *  - Write amplification, as there is no need to copy the whole content append.
  *  - Additional memory can be allocated as long as there is available memory in block size.
  *  - Deallocate unused space with no need to copy data.
  *
  * @note
  * The given set of Array composing the overall space may not be residing continuously in actual RAM.
  * The block size can be adjusted to optimize application performance.
  *
  * @param space     The given blob disk (RAM) space
  * @param totalSize The given blob space total size in bytes
  * @param size      The given blob used space in bytes
  * @param disk      The underlying [[InMemoryDisk]] for space allocation
  **/
private[storage] class InMemoryBlob(space: Vector[Array[Byte]], val totalSize: Int, val size: Int, disk: InMemoryDisk) {

  // --- Public Members --- //

  /** Available blob space to write in bytes */
  val available: Int = totalSize - size

  // --- Public Methods --- //

  /** Write binary content to blob
    *
    * @param content The given content to write
    * @param append  Whether to truncate the given content at the end of the given blob or override its data.
    * @return An updated [[InMemoryBlob]]
    */
  def write(content: Array[Byte], append: Boolean): InMemoryBlob = {
    if (append) appendContent(content, size)
    else overrideContent(content)
  }

  /** Reads the given blob */
  def read(): Iterator[Byte] = read(0, size)

  /** Reads `n` bytes from offset (inclusive)
    *
    * @param offset Start position to read from
    * @param n      Number of [[Byte]] to read from offset
    * @return An [[Iterator]] of block content at the given offset and length
    */
  def read(offset: Int, n: Int): Iterator[Byte] = new MemoryBlobIterator(space, size, disk.blockSize, offset, n)

  /** Strict read of given blob */
  def readStrict(): Array[Byte] = readStrict(0, size)

  /** Reads a portion of the given blob
    *
    * @param offset Start position to read from
    * @param n      Number of [[Byte]] to read from offset
    * @return Blobs content in as a single continues array from given offset and length
    */
  def readStrict(offset: Int, n: Int): Array[Byte] = {
    if (n < 0)
      throw new IllegalArgumentException("length must be non-negative")

    if (offset < 0)
      throw new IllegalArgumentException("offset position must be non-negative")

    if (offset + n > size)
      throw new IndexOutOfBoundsException(
        s"requested length '$n' from offset '$offset' exceeds current length of '$size'")

    cloneContent(space, offset, n, disk.blockSize)
  }

  /** Deallocate blob's space */
  def clear(): Unit = disk.deallocate(space)

  // --- Private Methods --- //

  private def appendContent(content: Array[Byte], offset: Int): InMemoryBlob = {
    val additionalSize = content.length - available
    if (additionalSize > 0) {
      val alloc = disk.allocate(additionalSize)
      val copied = copyContent(space ++ alloc, content, offset, disk.blockSize)
      new InMemoryBlob(copied, totalSize + calculateSize(alloc), totalSize + additionalSize, disk)
    } else {
      val copied = copyContent(space, content, offset, disk.blockSize)
      new InMemoryBlob(copied, totalSize, offset + content.length, disk)
    }
  }

  private def overrideContent(content: Array[Byte]): InMemoryBlob = {
    val allocation =
      if (content.length < totalSize) {
        disk.reallocate(space, content.length)
      } else if (content.length > totalSize) {
        space ++ disk.allocate(content.length - available)
      } else {
        space
      }
    val copied = copyContent(allocation, content, offset = 0, disk.blockSize)
    new InMemoryBlob(copied, calculateSize(copied), content.length, disk)
  }

  private def copyContent(space: Vector[Array[Byte]],
                          content: Array[Byte],
                          offset: Int,
                          block: Int): Vector[Array[Byte]] = {

    @tailrec
    def goCopy(cntItr: Iterator[Byte], i: Int, j: Int): Vector[Array[Byte]] = {
      if (!cntItr.hasNext) {
        space
      } else {
        space(i)(j) = cntItr.next()
        val (idx, pos) = if (j + 1 >= block) (i + 1, 0) else (i, j + 1)
        goCopy(cntItr, idx, pos)
      }
    }

    val (idx, pos) = calculateBlockPosition(offset, disk.blockSize)
    goCopy(content.iterator, idx, pos)
  }

  private def cloneContent(space: Vector[Array[Byte]], offset: Int, length: Int, block: Int): Array[Byte] = {
    val clone = new Array[Byte](length)

    @tailrec
    def goClone(c: Int, i: Int, j: Int): Array[Byte] = {
      if (c >= length) {
        clone
      } else {
        clone(c) = space(i)(j)
        val (idx, pos) = if (j + 1 >= block) (i + 1, 0) else (i, j + 1)
        goClone(c + 1, idx, pos)
      }
    }

    val (idx, pos) = calculateBlockPosition(offset, disk.blockSize)
    goClone(0, idx, pos)
  }

  private def calculateSize(vec: Vector[Array[Byte]]): Int = {
    vec.foldLeft(0)(_ + _.length)
  }
}

object InMemoryBlob {

  //--- Constructors ---//

  def empty(disk: InMemoryDisk): InMemoryBlob = {
    new InMemoryBlob(Vector.empty[Array[Byte]], 0, 0, disk)
  }

  def withContent(content: Array[Byte], disk: InMemoryDisk): InMemoryBlob = {
    empty(disk).write(content, append = false)
  }

  //--- Package Private Methods ---//

  private[storage] def calculateBlockPosition(offset: Int, blockSize: Int): (Int, Int) = {
    (offset / blockSize, offset % blockSize)
  }
}
