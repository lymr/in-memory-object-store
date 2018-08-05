package com.lymr.memos.storage

import com.lymr.memos.storage.InMemoryObjectStorageSuite._
import org.mockito.Mockito._
import org.mockito.{ArgumentMatcher, ArgumentMatchers, Mock, MockitoAnnotations}
import org.scalatest.{BeforeAndAfterEach, FunSuite}

class InMemoryObjectStorageSuite extends FunSuite with BeforeAndAfterEach {

  @Mock
  var mockDisk: InMemoryDisk = _

  var objectStorage: InMemoryObjectStorage = _

  override def beforeEach(): Unit = {

    super.beforeEach()

    MockitoAnnotations.initMocks(this)

    when(mockDisk.blockSize).thenReturn(BLOCK_SIZE)
    objectStorage = new InMemoryObjectStorage(mockDisk)
  }

  test("available memory calls underlying disk") {

    when(mockDisk.available).thenReturn(156)

    val available = objectStorage.memoryAvailable

    verify(mockDisk).available
    assertResult(156)(available)
  }

  test("used memory calls underlying disk") {

    when(mockDisk.used).thenReturn(192)

    val used = objectStorage.memoryUsed

    verify(mockDisk).used
    assertResult(192)(used)
  }

  test("total memory calls underlying disk") {

    when(mockDisk.totalCapacity).thenReturn(1024)

    val totalCapacity = objectStorage.memoryTotalCapacity

    verify(mockDisk).totalCapacity
    assertResult(1024)(totalCapacity)
  }

  test("adding new empty object to store") {

    objectStorage.add(id = 18)

    val objectMetadata = objectStorage.readMetadata(id = 18)

    assert(objectMetadata.isDefined)
  }

  test("adding new empty object with exiting id results with exception") {

    objectStorage.add(id = 18)

    intercept[IllegalStateException] {
      objectStorage.add(id = 18)
    }
  }

  test("removing object cleans it from storage and disk") {

    val inputContent = "some-content".getBytes
    when(mockDisk.allocate(inputContent.length)).thenReturn(InitializeSpace(inputContent.length))
    objectStorage.write(id = 18, content = inputContent, append = false)

    objectStorage.remove(id = 18)

    assertResult(None)(objectStorage.readMetadata(id = 18))
    verify(mockDisk).deallocate(argThat[Vector[Array[Byte]]](arg => arg.foldLeft(0)(_ + _.length) == 16))
  }

  test("read non exist object results with None") {

    val obj = objectStorage.read(id = 21)

    assertResult(None)(obj)
  }

  test("read non exist object with offset and count results with None") {

    val obj = objectStorage.read(id = 21, offset = 8, n = 2)

    assertResult(None)(obj)
  }

  test("read strict non exist object results with None") {

    val obj = objectStorage.readStrict(id = 21)

    assertResult(None)(obj)
  }

  test("read strict non exist object with offset and count results with None") {

    val obj = objectStorage.readStrict(id = 21, offset = 8, n = 2)

    assertResult(None)(obj)
  }

  test("read object update it access time") {

    val inputContent = "some-content"
    when(mockDisk.allocate(inputContent.length)).thenReturn(InitializeSpace(inputContent.length))
    objectStorage.write(id = 21, inputContent.getBytes, append = false)

    val objMetadata = objectStorage.readMetadata(id = 21)

    val content = objectStorage.readStrict(id = 21)

    val updatedObjMetadata = objectStorage.readMetadata(id = 21)

    assert(updatedObjMetadata.get.lastAccessTime > objMetadata.get.lastAccessTime)
  }

  test("read existing object returns it's data") {

    val inputContent = "some-content"
    when(mockDisk.allocate(inputContent.length)).thenReturn(InitializeSpace(inputContent.length))
    objectStorage.write(id = 21, inputContent.getBytes, append = false)

    val content = objectStorage.read(id = 21).map(_.toArray)

    assertResult(Some("some-content"))(content.map(new String(_)))
  }

  test("read strict existing object returns it data") {

    val inputContent = "some-content"
    when(mockDisk.allocate(inputContent.length)).thenReturn(InitializeSpace(inputContent.length))
    objectStorage.write(id = 21, inputContent.getBytes, append = false)

    val content = objectStorage.readStrict(id = 21)

    assertResult(Some("some-content"))(content.map(new String(_)))
  }

  test("read existing object with offset and count returns it data") {

    val inputContent = "some-content"
    when(mockDisk.allocate(inputContent.length)).thenReturn(InitializeSpace(inputContent.length))
    objectStorage.write(id = 21, inputContent.getBytes, append = false)

    val content = objectStorage.read(id = 21, offset = 5, n = 7).map(_.toArray)

    assertResult(Some("content"))(content.map(new String(_)))
  }

  test("read strict existing object with offset and count returns it data") {

    val inputContent = "some-content"
    when(mockDisk.allocate(inputContent.length)).thenReturn(InitializeSpace(inputContent.length))
    objectStorage.write(id = 21, inputContent.getBytes, append = false)

    val content = objectStorage.readStrict(id = 21, offset = 5, n = 7)

    assertResult(Some("content"))(content.map(new String(_)))
  }

  test("append to exist object updates object") {

    val inputContent = "some-string-content".getBytes
    val additionalContent = "-some-additional-test-content".getBytes
    val availableBlobSpace = calculateAvailableSpaceFor(inputContent.length)

    when(mockDisk.allocate(inputContent.length)).thenReturn(InitializeSpace(inputContent.length))
    when(mockDisk.allocate(additionalContent.length - availableBlobSpace))
      .thenReturn(InitializeSpace(additionalContent.length - availableBlobSpace))

    objectStorage.write(id = 21, inputContent, append = false)
    objectStorage.write(id = 21, additionalContent, append = true)

    val content = objectStorage.read(id = 21).map(_.toArray)

    assertResult(Some("some-string-content-some-additional-test-content"))(content.map(new String(_)))
  }

  test("override exist object updates object") {

    val inputContent = "some-string-content".getBytes
    val overrideContent = "some-bigger-override-content-to-test-:)".getBytes
    val availableBlobSpace = calculateAvailableSpaceFor(inputContent.length)

    when(mockDisk.allocate(inputContent.length)).thenReturn(InitializeSpace(inputContent.length))
    when(mockDisk.allocate(overrideContent.length - availableBlobSpace))
      .thenReturn(InitializeSpace(overrideContent.length - availableBlobSpace))

    objectStorage.write(id = 21, inputContent, append = false)
    objectStorage.write(id = 21, overrideContent, append = false)

    val content = objectStorage.read(id = 21).map(_.toArray)

    assertResult(Some("some-bigger-override-content-to-test-:)"))(content.map(new String(_)))
  }

  test("object size returns actual size") {

    val inputContent = "some-content"
    when(mockDisk.allocate(inputContent.length)).thenReturn(InitializeSpace(inputContent.length))
    objectStorage.write(id = 21, inputContent.getBytes, append = false)

    val size = objectStorage.objectSize(id = 21)

    assertResult(Some(12))(size)
  }

  test("object size on disk returns it total size") {

    val inputContent = "some-content"
    when(mockDisk.allocate(inputContent.length)).thenReturn(InitializeSpace(inputContent.length))
    objectStorage.write(id = 21, inputContent.getBytes, append = false)

    val size = objectStorage.objectSizeOnDisk(id = 21)

    assertResult(Some(16))(size)
  }

  test("object size for non exist object returns None") {

    val size = objectStorage.objectSize(id = 21)

    assertResult(None)(size)
  }

  test("object size on disk for non exist object returns None") {

    val size = objectStorage.objectSizeOnDisk(id = 21)

    assertResult(None)(size)
  }

  private def InitializeSpace(size: Int): Vector[Array[Byte]] = {
    Array.fill[Byte](calculateBlocksNumber(size, BLOCK_SIZE), BLOCK_SIZE)(0).toVector
  }

  private def calculateBlocksNumber(size: Int, blockSize: Int): Int = {
    (size / blockSize) + (if (size % blockSize == 0) 0 else 1)
  }

  private def calculateAvailableSpaceFor(size: Int): Int = {
    calculateBlocksNumber(size, BLOCK_SIZE) * BLOCK_SIZE - size
  }

  private def argThat[A](f: A => Boolean): A = {

    ArgumentMatchers.argThat(new ArgumentMatcher[A] {
      override def matches(argument: A): Boolean = f(argument)
    })
  }
}

object InMemoryObjectStorageSuite {
  val BLOCK_SIZE = 8
}
