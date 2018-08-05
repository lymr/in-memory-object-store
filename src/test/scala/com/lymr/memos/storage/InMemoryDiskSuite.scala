package com.lymr.memos.storage

import org.scalatest.FunSuite

class InMemoryDiskSuite extends FunSuite {

  test("create with block size smaller than minimum results with exception") {

    intercept[IllegalArgumentException] {
      InMemoryDisk.withBlocks(numberOfBlocks = 8, blockSize = 63)
    }
  }

  test("create with non-positive blocks number results with exception") {

    intercept[IllegalArgumentException] {
      InMemoryDisk.withBlocks(numberOfBlocks = 0, blockSize = 64)
    }
  }

  test("disk block size equals to given") {

    val disk = InMemoryDisk.withBlocks(numberOfBlocks = 8, blockSize = 64)

    assertResult(64)(disk.blockSize)
  }

  test("used returns zero when no data is allocated") {

    val disk = InMemoryDisk.withBlocks(numberOfBlocks = 8, blockSize = 64)

    assertResult(0)(disk.used)
  }

  test("used returns total amount of allocated byes") {

    val disk = InMemoryDisk.withBlocks(numberOfBlocks = 8, blockSize = 64)

    val alloc = disk.allocate(121)

    assertResult(128)(disk.used)
  }

  test("totalCapacity returns total amount of space that can be allocated by given blocks") {

    val disk = InMemoryDisk.withBlocks(numberOfBlocks = 8, blockSize = 64)

    assertResult(8 * 64)(disk.totalCapacity)
  }

  test("available returns total capacity when no space is allocated") {

    val disk = InMemoryDisk.withBlocks(numberOfBlocks = 8, blockSize = 64)

    assertResult(8 * 64)(disk.available)
  }

  test("available returns total capacity minus used capacity") {

    val disk = InMemoryDisk.withBlocks(numberOfBlocks = 8, blockSize = 64)

    val alloc = disk.allocate(121)

    assertResult(6 * 64)(disk.available)
  }

  test("canAllocate returns true for size less than available size") {

    val disk = InMemoryDisk.withBlocks(numberOfBlocks = 8, blockSize = 64)

    assert(disk.canAllocate(121))
  }

  test("canAllocate returns true for size equals to available size") {

    val disk = InMemoryDisk.withBlocks(numberOfBlocks = 2, blockSize = 64)

    assert(disk.canAllocate(128))
  }

  test("canAllocate returns false for size larger than available size") {

    val disk = InMemoryDisk.withBlocks(numberOfBlocks = 2, blockSize = 64)

    assert(!disk.canAllocate(142))
  }

  test("allocate for negative size results with exception") {

    val disk = InMemoryDisk.withBlocks(numberOfBlocks = 2, blockSize = 64)

    intercept[IllegalArgumentException] {
      disk.allocate(-28)
    }
  }

  test("try to allocate size larger than available results with exception") {

    val disk = InMemoryDisk.withBlocks(numberOfBlocks = 2, blockSize = 64)

    intercept[OutOfDiskMemoryException] {
      disk.allocate(256)
    }
  }

  test("allocate space returns aligned space to block size") {

    val disk = InMemoryDisk.withBlocks(numberOfBlocks = 8, blockSize = 64)

    val alloc = disk.allocate(198)

    assertResult(256)(alloc.foldLeft(0)(_ + _.length))
  }

  test("allocate space which is multiply of block size") {

    val disk = InMemoryDisk.withBlocks(numberOfBlocks = 8, blockSize = 64)

    val alloc = disk.allocate(256)

    assertResult(256)(alloc.foldLeft(0)(_ + _.length))
  }

  test("reallocate a larger space than given") {

    val disk = InMemoryDisk.withBlocks(numberOfBlocks = 8, blockSize = 64)

    val alloc = disk.allocate(256)

    intercept[IllegalArgumentException] {
      disk.reallocate(alloc, 312)
    }
  }

  test("reallocate an exactly the space given") {

    val disk = InMemoryDisk.withBlocks(numberOfBlocks = 8, blockSize = 64)

    val alloc = disk.allocate(256)

    val realloc = disk.reallocate(alloc, 256)

    assertResult(256)(realloc.foldLeft(0)(_ + _.length))
  }

  test("reallocate a smaller space than given") {

    val disk = InMemoryDisk.withBlocks(numberOfBlocks = 8, blockSize = 64)

    val alloc = disk.allocate(256)

    val realloc = disk.reallocate(alloc, 186)

    assertResult(192)(realloc.foldLeft(0)(_ + _.length))
  }

  test("reallocate returns space to disk") {

    val disk = InMemoryDisk.withBlocks(numberOfBlocks = 8, blockSize = 64)

    val alloc = disk.allocate(256)

    val realloc = disk.reallocate(alloc, 186)

    assertResult(5 * 64)(disk.available)
  }

  test("allocated space is initialized to zeros") {

    val disk = InMemoryDisk.withBlocks(numberOfBlocks = 8, blockSize = 64)

    val alloc = disk.allocate(256)

    assertResult(0)(alloc.foldLeft(0)(_ + _.sum))
  }

  test("reallocate an exactly the space given initializes space to zeros") {

    val disk = InMemoryDisk.withBlocks(numberOfBlocks = 8, blockSize = 64)

    val alloc = disk.allocate(256)

    val realloc = disk.reallocate(alloc, 256)

    assertResult(0)(realloc.foldLeft(0)(_ + _.sum))
  }

  test("reallocate a smaller space than given initializes space to zeros") {

    val disk = InMemoryDisk.withBlocks(numberOfBlocks = 8, blockSize = 64)

    val alloc = disk.allocate(256)

    val realloc = disk.reallocate(alloc, 186)

    assertResult(0)(realloc.foldLeft(0)(_ + _.sum))
  }

  test("deallocate returns space to disk") {

    val disk = InMemoryDisk.withBlocks(numberOfBlocks = 8, blockSize = 64)

    val alloc = disk.allocate(256)

    disk.deallocate(alloc)

    assertResult(8 * 64)(disk.available)
  }

  test("create disk with capacity") {

    val disk = InMemoryDisk.withCapacity(1024, 64)

    assertResult(1024)(disk.available)
  }
}
