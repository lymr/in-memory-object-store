package com.lymr.memos.storage

import com.lymr.memos.storage.InMemoryDisk._

/** An in memory disk with predefined capacity and fixed size memory blocks.
  *
  * @note
  * DiskCapacity(Bytes) = numberOfBlocks * blockSize
  * Minimum block size = 64 byte
  *
  * @param numberOfBlocks Total number of RAM blocks to be utilized by this disk
  * @param blockSize      RAM block size in bytes
  */
private[memos] class InMemoryDisk(numberOfBlocks: Int, val blockSize: Int) {
  require(blockSize >= MIN_BLOCK_SIZE, s"block size must be grater than '$MIN_BLOCK_SIZE' bytes")
  require(numberOfBlocks > 0, "in memory disk must must have at least one block")

  //--- Private Members ---//

  private var allocatedBlocks: Int = 0

  //--- Public Methods ---//

  /** Disk total capacity in bytes */
  def totalCapacity: Int = numberOfBlocks * blockSize

  /** Used disk space in bytes */
  def used: Int = allocatedBlocks * blockSize

  /** Available disk space in bytes */
  def available: Int = totalCapacity - used

  /** True whether a block of the given size can be allocated */
  def canAllocate(size: Int): Boolean = available >= size

  /** Allocates one or more fixed sized blocks to be used, in case when requested size is not
    * a multiply of the fixed block size this methods rounds up the number of allocated blocks.
    *
    * @param size Size to allocate
    * @throws OutOfDiskMemoryException in case when disk is full and unable to allocate space
    * @return A newly allocated space aligned to block size
    */
  @throws[IllegalArgumentException]
  @throws[OutOfDiskMemoryException]
  def allocate(size: Int): Vector[Array[Byte]] = {
    if (size < 0)
      throw new IllegalArgumentException("requested allocation size must be non-negative")

    if (!canAllocate(size))
      throw OutOfDiskMemoryException(s"failed to allocate $size, available capacity:= $available")

    val requiredBlocks = calculateBlocks(size, blockSize)
    val allocatedSpace = Vector.tabulate(requiredBlocks)(_ => new Array[Byte](blockSize))
    allocatedBlocks += requiredBlocks
    allocatedSpace
  }

  /** Shrinks the given space to a new given size, in case when requested size is not a multiply
    * of the fixed block size this methods floor the number of deallocate blocks.
    *
    * @param space   The given space to shrink
    * @param newSize New space size
    * @return A newly allocated space aligned to block size
    */
  def reallocate(space: Vector[Array[Byte]], newSize: Int): Vector[Array[Byte]] = {
    if (newSize > space.foldLeft(0)(_ + _.length))
      throw new IllegalArgumentException("new size must be smaller then given space size")

    val requiredBlocks = calculateBlocks(newSize, blockSize)
    val unusedBlocks = space.size - requiredBlocks
    val newSpace = initializeSpace(space.take(requiredBlocks))
    allocatedBlocks -= unusedBlocks
    newSpace
  }

  /** Deallocate the given space, return it back to disk */
  def deallocate(space: Vector[Array[Byte]]): Unit = {
    initializeSpace(space)
    allocatedBlocks -= space.size
  }
}

object InMemoryDisk {

  // --- Constants --- //

  private val MIN_BLOCK_SIZE: Int = 64

  //--- Constructors ---//

  /** Initialize a new [[InMemoryDisk]] with given number of blocks and fixed block size
    *
    * @param numberOfBlocks Number of block in newly created in-memory disk
    * @param blockSize      In-memory disk block block size
    * @return A newly created [[InMemoryDisk]]
    */
  def withBlocks(numberOfBlocks: Int, blockSize: Int): InMemoryDisk = {
    new InMemoryDisk(numberOfBlocks, blockSize)
  }

  /** Initialize a new [[InMemoryDisk]] with given total capacity and fixed block size.
    *
    * @param capacity  In-memory disk total capacity in [[Byte]]
    * @param blockSize In-memory disk block block size
    * @return A newly created [[InMemoryDisk]]
    */
  def withCapacity(capacity: Int, blockSize: Int): InMemoryDisk = {
    val requiredBlocks = calculateBlocks(capacity, blockSize)
    new InMemoryDisk(requiredBlocks, blockSize)
  }

  // --- Private Methods --- //

  /** Calculates the number of fixed size blocks needed for the given size
    *
    * @param requiredSize Desired space size
    * @param blockSize    [[InMemoryDisk]] fixed block size
    * @return Number of blocks needed to allocated for the given space size
    */
  private def calculateBlocks(requiredSize: Int, blockSize: Int): Int = {
    requiredSize / blockSize + (if (requiredSize % blockSize == 0) 0 else 1)
  }

  /** Initialize space writing zero to all bytes
    *
    * @param space Given space to initialize
    * @return An empty space
    */
  private def initializeSpace(space: Vector[Array[Byte]]): Vector[Array[Byte]] = {
    space.foreach(blk => for (i <- blk.indices) blk(i) = 0)
    space
  }
}

final case class OutOfDiskMemoryException(message: String) extends Exception
