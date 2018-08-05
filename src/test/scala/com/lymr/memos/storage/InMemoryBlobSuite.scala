package com.lymr.memos.storage

import com.lymr.memos.storage.InMemoryBlobSuite._
import org.mockito.ArgumentMatchers.{any, eq => equal}
import org.mockito.Mockito._
import org.mockito.{Mock, MockitoAnnotations}
import org.scalatest.{BeforeAndAfterEach, FunSuite}

class InMemoryBlobSuite extends FunSuite with BeforeAndAfterEach {

  @Mock
  var mockDisk: InMemoryDisk = _

  override def beforeEach(): Unit = {
    super.beforeEach()

    MockitoAnnotations.initMocks(this)

    when(mockDisk.blockSize).thenReturn(BLOCK_SIZE)
  }

  test("empty blob size equals zero") {

    val blob = InMemoryBlob.empty(mockDisk)

    assertResult(0)(blob.size)
  }

  test("empty blob total size equals zero") {

    val blob = InMemoryBlob.empty(mockDisk)

    assertResult(0)(blob.totalSize)
  }

  test("empty blob available size equals zero") {

    val blob = InMemoryBlob.empty(mockDisk)

    assertResult(0)(blob.available)
  }

  test("blob with non empty content size") {

    val content = "some-string-content".getBytes
    when(mockDisk.allocate(content.length)).thenReturn(InitializeSpace(content.length))

    val blob = InMemoryBlob.withContent(content, mockDisk)

    assertResult(content.length)(blob.size)
  }

  test("blob with non empty content total size aligned with block size") {

    val content = "some-string-content".getBytes
    when(mockDisk.allocate(content.length)).thenReturn(InitializeSpace(content.length))

    val blob = InMemoryBlob.withContent(content, mockDisk)

    assertResult(calculateBlocksNumber(content.length, BLOCK_SIZE) * BLOCK_SIZE)(blob.totalSize)
  }

  test("blob with non empty content available size") {

    val content = "some-string-content".getBytes
    when(mockDisk.allocate(content.length)).thenReturn(InitializeSpace(content.length))

    val blob = InMemoryBlob.withContent(content, mockDisk)

    val expected = (calculateBlocksNumber(content.length, BLOCK_SIZE) * BLOCK_SIZE) - content.length
    assertResult(expected)(blob.available)
  }

  test("full read empty blob results with empty iterator") {

    val blob = InMemoryBlob.empty(mockDisk)

    val contentIterator = blob.read()

    assert(!contentIterator.hasNext)
  }

  test("read zero bytes from non-empty blob") {

    val inputContent = "some-string-content".getBytes
    when(mockDisk.allocate(inputContent.length)).thenReturn(InitializeSpace(inputContent.length))
    val blob = InMemoryBlob.withContent(inputContent, mockDisk)

    val contentIterator = blob.read(offset = 5, n = 0)

    assert(!contentIterator.hasNext)
  }

  test("read 3 bytes from non-empty blob") {

    val inputContent = "some-string-content".getBytes
    when(mockDisk.allocate(inputContent.length)).thenReturn(InitializeSpace(inputContent.length))
    val blob = InMemoryBlob.withContent(inputContent, mockDisk)

    val contentIterator = blob.read(offset = 6, n = 5)

    assertResult(Array[Byte](116, 114, 105, 110, 103))(contentIterator.toArray)
  }

  test("full strict read empty blob results with empty iterator") {

    val blob = InMemoryBlob.empty(mockDisk)

    val content = blob.readStrict()

    assertResult(Array.emptyByteArray)(content)
  }

  test("read strict zero bytes from non-empty blob") {

    val inputContent = "some-string-content".getBytes
    when(mockDisk.allocate(inputContent.length)).thenReturn(InitializeSpace(inputContent.length))
    val blob = InMemoryBlob.withContent(inputContent, mockDisk)

    val content = blob.readStrict(offset = 5, n = 0)

    assertResult(Array.emptyByteArray)(content)
  }

  test("read strict 3 bytes from non-empty blob") {

    val inputContent = "some-string-content".getBytes
    when(mockDisk.allocate(inputContent.length)).thenReturn(InitializeSpace(inputContent.length))
    val blob = InMemoryBlob.withContent(inputContent, mockDisk)

    val content = blob.readStrict(offset = 6, n = 5)

    assertResult(Array[Byte](116, 114, 105, 110, 103))(content)
  }

  test("read more than blob size result with exception") {

    val inputContent = "some-string-content".getBytes
    when(mockDisk.allocate(any())).thenReturn(InitializeSpace(inputContent.length))

    val blob = InMemoryBlob.withContent(inputContent, mockDisk)

    intercept[IndexOutOfBoundsException] {
      blob.read(offset = 9, n = 20)
    }
  }

  test("read strict more than blob size result with exception") {

    val inputContent = "some-string-content".getBytes
    when(mockDisk.allocate(any())).thenReturn(InitializeSpace(inputContent.length))

    val blob = InMemoryBlob.withContent(inputContent, mockDisk)

    intercept[IndexOutOfBoundsException] {
      blob.readStrict(offset = 11, n = 20)
    }
  }

  test("read negative number of bytes results with exception") {

    val blob = InMemoryBlob.empty(mockDisk)

    intercept[IllegalArgumentException] {
      blob.read(offset = 3, n = -1)
    }
  }
  test("read strict negative number of bytes results with exception") {

    val blob = InMemoryBlob.empty(mockDisk)

    intercept[IllegalArgumentException] {
      blob.readStrict(offset = 2, n = -4)
    }
  }

  test("read from negative start position results with exception") {

    val blob = InMemoryBlob.empty(mockDisk)

    intercept[IllegalArgumentException] {
      blob.read(offset = -1, n = 1)
    }
  }

  test("read strict from negative start position results with exception") {

    val blob = InMemoryBlob.empty(mockDisk)

    intercept[IllegalArgumentException] {
      blob.readStrict(offset = -3, n = 1)
    }
  }

  test("read strict full blob size returns all content") {

    val inputContent = "some-string-content".getBytes
    when(mockDisk.allocate(inputContent.length)).thenReturn(InitializeSpace(inputContent.length))
    val blob = InMemoryBlob.withContent(inputContent, mockDisk)

    val resultContent = blob.readStrict()

    assertResult("some-string-content")(new String(resultContent))
  }

  test("read strict some blob's bytes from position zero") {

    val inputContent = "some-string-content".getBytes
    when(mockDisk.allocate(inputContent.length)).thenReturn(InitializeSpace(inputContent.length))
    val blob = InMemoryBlob.withContent(inputContent, mockDisk)

    val resultContent = blob.readStrict(offset = 0, n = 11)

    assertResult("some-string")(new String(resultContent))
  }

  test("read strict some blob's data from a positive position") {

    val inputContent = "some-string-content".getBytes
    when(mockDisk.allocate(inputContent.length)).thenReturn(InitializeSpace(inputContent.length))

    val blob = InMemoryBlob.withContent(inputContent, mockDisk)
    val resultContent = blob.readStrict(offset = 5, n = 6)

    assertResult("string")(new String(resultContent))
  }

  test("read after append returns updated content") {
    val inputContent = "some-string-content".getBytes
    val additionalContent = "-some-additional-test-content".getBytes
    val availableBlobSpace = calculateAvailableSpaceFor(inputContent.length)

    when(mockDisk.allocate(inputContent.length)).thenReturn(InitializeSpace(inputContent.length))
    when(mockDisk.allocate(additionalContent.length - availableBlobSpace))
      .thenReturn(InitializeSpace(additionalContent.length - availableBlobSpace))

    val initBlob = InMemoryBlob.withContent(inputContent, mockDisk)
    val updatedBlob = initBlob.write(additionalContent, append = true)
    val resultContent = updatedBlob.read().toArray

    assertResult("some-string-content-some-additional-test-content")(new String(resultContent))
  }

  test("read after append when new content size fits in current returns updated content") {
    val inputContent = "some-string-content".getBytes
    val additionalContent = "-test".getBytes

    when(mockDisk.allocate(inputContent.length)).thenReturn(InitializeSpace(inputContent.length))

    val initBlob = InMemoryBlob.withContent(inputContent, mockDisk)
    val updatedBlob = initBlob.write(additionalContent, append = true)
    val resultContent = updatedBlob.read().toArray

    assertResult("some-string-content-test")(new String(resultContent))
  }

  test("read after override with bigger content returns updated content") {
    val inputContent = "some-string-content".getBytes
    val overrideContent = "some-bigger-override-content-to-test-:)".getBytes
    val availableBlobSpace = calculateAvailableSpaceFor(inputContent.length)

    when(mockDisk.allocate(inputContent.length)).thenReturn(InitializeSpace(inputContent.length))
    when(mockDisk.allocate(overrideContent.length - availableBlobSpace))
      .thenReturn(InitializeSpace(overrideContent.length - availableBlobSpace))

    val initBlob = InMemoryBlob.withContent(inputContent, mockDisk)
    val updatedBlob = initBlob.write(overrideContent, append = false)
    val resultContent = updatedBlob.readStrict()

    assertResult("some-bigger-override-content-to-test-:)")(new String(resultContent))
  }

  test("read after override with exactly allocated size content returns updated content") {
    val inputContent = "some-string-content".getBytes
    val overrideContent = "another-str-content-test".getBytes

    when(mockDisk.allocate(inputContent.length)).thenReturn(InitializeSpace(inputContent.length))
    when(mockDisk.reallocate(any(), equal(overrideContent.length))).thenReturn(InitializeSpace(overrideContent.length))

    val initBlob = InMemoryBlob.withContent(inputContent, mockDisk)
    val updatedBlob = initBlob.write(overrideContent, append = false)
    val resultContent = updatedBlob.readStrict()

    assertResult("another-str-content-test")(new String(resultContent))
  }

  test("read after override with smaller size content returns updated content") {
    val inputContent = "some-string-content".getBytes
    val overrideContent = "smaller".getBytes

    when(mockDisk.allocate(inputContent.length)).thenReturn(InitializeSpace(inputContent.length))
    when(mockDisk.reallocate(any(), equal(overrideContent.length))).thenReturn(InitializeSpace(overrideContent.length))

    val initBlob = InMemoryBlob.withContent(inputContent, mockDisk)
    val updatedBlob = initBlob.write(overrideContent, append = false)
    val resultContent = updatedBlob.readStrict()

    assertResult("smaller")(new String(resultContent))
  }

  test("clear deallocate blob's space to disk") {

    val inputContent = "some-string-content".getBytes
    val space = InitializeSpace(inputContent.length)
    when(mockDisk.allocate(any())).thenReturn(space)
    val blob = InMemoryBlob.withContent(inputContent, mockDisk)

    blob.clear()

    verify(mockDisk).deallocate(space)
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
}

object InMemoryBlobSuite {
  val BLOCK_SIZE = 8
}
