package com.lymr.memos.storage

import com.lymr.memos.storage.InMemoryBlob.calculateBlockPosition

/** A two dimensional array iterator where the first dimension has a fixed size, the iterator supports starting
  * from an initial position and taking `n` elements.
  *
  * @param space     Blob's space to iterate over.
  * @param size      Actual blob's size.
  * @param blockSize Fist dimension size.
  * @param start     A position to start iterate from.
  * @param n         Number of elements to iterate over.
  */
private[storage] class MemoryBlobIterator(space: Vector[Array[Byte]], size: Int, blockSize: Int, start: Int, var n: Int)
    extends Iterator[Byte] {

  require(n >= 0, "requested count must be positive number")
  require(start >= 0, "offset position must be non-negative")

  if (size < start + n)
    throw new IndexOutOfBoundsException(s"requested length '$n' from offset '$start' exceeds current length of '$size'")

  //--- Private Members ---//

  private var (idx, pos) = calculateBlockPosition(start, blockSize)

  //--- Public Methods ---//

  override def hasNext: Boolean = n > 0

  override def next(): Byte = {
    val cur = space(idx)(pos)
    n -= 1
    pos += 1
    if (pos >= blockSize) {
      idx += 1
      pos = 0
    }
    cur
  }
}
