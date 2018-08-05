package com.lymr.memos.entities

import com.lymr.memos.InMemoryObjectStore
import org.mockito.ArgumentMatchers.{eq => equal}
import org.mockito.Mockito._
import org.mockito.{Mock, MockitoAnnotations}
import org.scalatest.{BeforeAndAfterEach, FunSuite}

class StoreEntitySuite extends FunSuite with BeforeAndAfterEach {

  @Mock
  var mockObjectStore: InMemoryObjectStore = _

  override def beforeEach(): Unit = {

    super.beforeEach()

    MockitoAnnotations.initMocks(this)
  }

  test("object creationTime calls underlying object store") {

    MemoryObject(2, 1, "test-object", mockObjectStore).createTime

    verify(mockObjectStore).createTime(equal(2))
  }

  test("object lastAccessTime calls underlying object store") {

    MemoryObject(2, 1, "test-object", mockObjectStore).lastAccessTime

    verify(mockObjectStore).lastAccessTime(equal(2))
  }

  test("object lastModificationTime calls underlying object store") {

    MemoryObject(2, 1, "test-object", mockObjectStore).lastModificationTime

    verify(mockObjectStore).lastModificationTime(equal(2))
  }

  test("isObject for object returns true") {

    assert(MemoryObject(2, 1, "test-object", mockObjectStore).isObject)
  }

  test("isContainer for object returns false") {

    assert(!MemoryObject(2, 1, "test-object", mockObjectStore).isContainer)
  }

  test("isBucket for object calls underlying object store") {

    MemoryObject(2, 1, "test-object", mockObjectStore).isBucket

    verify(mockObjectStore).isBucket(equal(2))
  }

  test("size for object calls underlying object store") {

    MemoryObject(2, 1, "test-object", mockObjectStore).size

    verify(mockObjectStore).size(equal(2))
  }

  test("parent for object calls underlying object store") {

    MemoryObject(2, 1, "test-object", mockObjectStore).parent

    verify(mockObjectStore).findParent(equal(1))
  }

  test("updating container last access time saves given time") {

    val container = Container(2, 1, "test-container", mockObjectStore)

    container.updateAccessTime(1234L)

    assertResult(1234L)(container.lastAccessTime)
  }

  test("updating container last modification time saves given time") {

    val container = Container(2, 1, "test-container", mockObjectStore)

    container.updateModificationTime(1234L)

    assertResult(1234L)(container.lastModificationTime)
  }

  test("isObject for container returns false") {

    assert(!Container(2, 1, "test-container", mockObjectStore).isObject)
  }

  test("isContainer for container returns true") {

    assert(Container(2, 1, "test-container", mockObjectStore).isContainer)
  }

  test("isBucket for container calls underlying object store") {

    Container(2, 1, "test-container", mockObjectStore).isBucket

    verify(mockObjectStore).isBucket(equal(2))
  }

  test("size for container calls underlying object store") {

    Container(2, 1, "test-container", mockObjectStore).size

    verify(mockObjectStore).size(equal(2))
  }

  test("parent for container calls underlying object store") {

    Container(2, 1, "test-container", mockObjectStore).parent

    verify(mockObjectStore, times(1)).findParent(equal(1))
  }
}
