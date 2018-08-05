package com.lymr.memos.structure

import com.lymr.memos.InMemoryObjectStore
import com.lymr.memos.entities.{Container, MemoryObject, StoreEntity}
import org.mockito.{Mock, MockitoAnnotations}
import org.scalatest.{BeforeAndAfterEach, FunSuite}

class ObjectStoreTreeSuite extends FunSuite with BeforeAndAfterEach {

  @Mock
  var mockObjectStore: InMemoryObjectStore = _

  var entitiesTree: StoreEntitiesTree = _

  override def beforeEach(): Unit = {

    super.beforeEach()

    MockitoAnnotations.initMocks(this)

    entitiesTree = new StoreEntitiesTree()
  }

  test("add container with no parent as bucket") {

    val bucket = Container(1, -1, "bucket", mockObjectStore)

    entitiesTree.add(bucket)

    assert(entitiesTree.isBucket(bucket.id))
  }

  test("add container with parent") {

    val bucket = Container(1, -1, "bucket", mockObjectStore)
    val container = Container(2, 1, "sub-container", mockObjectStore)
    entitiesTree.add(bucket)
    entitiesTree.add(container, bucket.id)

    val result = entitiesTree.get(container.id)

    assertResult(Some(container))(result)
  }

  test("add container when parent bucket does not exist results with exception") {

    val bucket = Container(1, -1, "bucket", mockObjectStore)
    val container = Container(2, 1, "sub-container", mockObjectStore)

    intercept[IllegalStateException] {
      entitiesTree.add(container, bucket.id)
    }
  }

  test("add container when parent container does not exist results with exception") {

    val container = Container(2, 1, "sub-container", mockObjectStore)
    val container2 = Container(3, 1, "sub-container-2", mockObjectStore)

    intercept[IllegalStateException] {
      entitiesTree.add(container2, container.id)
    }
  }

  test("add container to itself results with exception") {

    val container = Container(2, 1, "sub-container", mockObjectStore)

    intercept[IllegalStateException] {
      entitiesTree.add(container, container.id)
    }
  }

  test("add object to itself results with exception") {

    val memObj = MemoryObject(2, 1, "sub-object", mockObjectStore)

    intercept[IllegalStateException] {
      entitiesTree.add(memObj, memObj.id)
    }
  }

  test("add bucket to container results with exception") {

    val bucket = Container(1, -1, "bucket", mockObjectStore)
    val container = Container(2, 1, "sub-container", mockObjectStore)

    entitiesTree.add(bucket)

    intercept[IllegalStateException] {
      entitiesTree.add(bucket, container.id)
    }
  }

  test("add entity with existing id results with exception") {

    val bucket = Container(1, -1, "bucket", mockObjectStore)
    val memObj = MemoryObject(1, 1, "sub-object", mockObjectStore)
    entitiesTree.add(bucket)

    intercept[IllegalStateException] {
      entitiesTree.add(memObj, bucket.id)
    }
  }

  test("add object to bucket") {

    val bucket = Container(1, -1, "bucket", mockObjectStore)
    val memObj = MemoryObject(2, 1, "sub-object", mockObjectStore)
    entitiesTree.add(bucket)
    entitiesTree.add(memObj, bucket.id)

    val result = entitiesTree.get(memObj.id)

    assertResult(Some(memObj))(result)
  }

  test("add object to container") {

    val bucket = Container(1, -1, "bucket", mockObjectStore)
    val container = Container(2, 1, "sub-container", mockObjectStore)
    val memObj = MemoryObject(3, 1, "sub-object", mockObjectStore)
    entitiesTree.add(bucket)
    entitiesTree.add(container, bucket.id)
    entitiesTree.add(memObj, container.id)

    val result = entitiesTree.get(memObj.id)

    assertResult(Some(memObj))(result)
  }

  test("add object when parent does not exist results with exception") {

    val container = Container(2, 1, "sub-container", mockObjectStore)
    val memObj = MemoryObject(3, 1, "sub-object", mockObjectStore)

    intercept[IllegalStateException] {
      entitiesTree.add(memObj, container.id)
    }
  }

  test("list all bucket containers") {

    val bucket1 = Container(1, -1, "bucket-1", mockObjectStore)
    val bucket2 = Container(2, -1, "bucket-2", mockObjectStore)
    entitiesTree.add(bucket1)
    entitiesTree.add(bucket2)

    val buckets = entitiesTree.listBuckets()

    assertResult(Set(bucket1, bucket2))(buckets)
  }

  test("list all bucket containers when tree is empty") {

    val buckets = entitiesTree.listBuckets()

    assertResult(Set.empty[Container])(buckets)
  }

  test("list empty tree") {

    val entities = entitiesTree.list(id = 1, recursive = true)

    assertResult(Set.empty[StoreEntity])(entities)
  }

  test("list recursive non empty tree returns all entities") {

    val bucket = Container(1, -1, "bucket", mockObjectStore)
    val container = Container(2, 1, "sub-container-1", mockObjectStore)
    val container2 = Container(3, 2, "sub-container-2", mockObjectStore)
    val memObj = MemoryObject(4, 1, "sub-object-1", mockObjectStore)
    val memObj2 = MemoryObject(5, 1, "sub-object-2", mockObjectStore)
    entitiesTree.add(bucket)
    entitiesTree.add(container, bucket.id)
    entitiesTree.add(container2, container.id)
    entitiesTree.add(memObj, container.id)
    entitiesTree.add(memObj2, container2.id)

    val entities = entitiesTree.list(1, recursive = true)

    assertResult(Set(container, container2, memObj, memObj2))(entities)
  }

  test("list container non recursive results with first level entities") {

    val bucket = Container(1, -1, "bucket", mockObjectStore)
    val container = Container(2, 1, "sub-container-1", mockObjectStore)
    val container2 = Container(3, 2, "sub-container-2", mockObjectStore)
    val memObj = MemoryObject(4, 1, "sub-object-1", mockObjectStore)
    val memObj2 = MemoryObject(5, 1, "sub-object-2", mockObjectStore)
    entitiesTree.add(bucket)
    entitiesTree.add(container, bucket.id)
    entitiesTree.add(container2, container.id)
    entitiesTree.add(memObj, container.id)
    entitiesTree.add(memObj2, container2.id)

    val entities = entitiesTree.list(2, recursive = true)

    assertResult(Set(container2, memObj, memObj2))(entities)
  }

  test("listObjects for tree with no objects results with an empty list") {

    val bucket = Container(1, -1, "bucket", mockObjectStore)
    val container = Container(2, 1, "sub-container-1", mockObjectStore)
    val container2 = Container(3, 2, "sub-container-2", mockObjectStore)
    entitiesTree.add(bucket)
    entitiesTree.add(container, bucket.id)
    entitiesTree.add(container2, container.id)

    val entities = entitiesTree.listObjects(1, recursive = true)

    assertResult(Set.empty[StoreEntity])(entities)
  }

  test("listObjects recursive returns all objects") {

    val bucket = Container(1, -1, "bucket", mockObjectStore)
    val container = Container(2, 1, "sub-container-1", mockObjectStore)
    val container2 = Container(3, 2, "sub-container-2", mockObjectStore)
    val memObj = MemoryObject(4, 1, "sub-object-1", mockObjectStore)
    val memObj2 = MemoryObject(5, 1, "sub-object-2", mockObjectStore)
    entitiesTree.add(bucket)
    entitiesTree.add(container, bucket.id)
    entitiesTree.add(container2, container.id)
    entitiesTree.add(memObj, container.id)
    entitiesTree.add(memObj2, container2.id)

    val entities = entitiesTree.listObjects(1, recursive = true)

    assertResult(Set(memObj, memObj2))(entities)
  }

  test("listObjects non recursive results with first level objects") {

    val bucket = Container(1, -1, "bucket", mockObjectStore)
    val container = Container(2, 1, "sub-container-1", mockObjectStore)
    val container2 = Container(3, 2, "sub-container-2", mockObjectStore)
    val memObj = MemoryObject(4, 1, "sub-object-1", mockObjectStore)
    val memObj2 = MemoryObject(5, 1, "sub-object-2", mockObjectStore)
    entitiesTree.add(bucket)
    entitiesTree.add(container, bucket.id)
    entitiesTree.add(container2, container.id)
    entitiesTree.add(memObj, container.id)
    entitiesTree.add(memObj2, container2.id)

    val entities = entitiesTree.listObjects(2, recursive = false)

    assertResult(Set(memObj))(entities)
  }

  test("listObjects non recursive with no sub objects results with empty list") {

    val bucket = Container(1, -1, "bucket", mockObjectStore)
    val container = Container(2, 1, "sub-container-1", mockObjectStore)
    val container2 = Container(3, 2, "sub-container-2", mockObjectStore)
    val memObj = MemoryObject(4, 1, "sub-object-1", mockObjectStore)
    val memObj2 = MemoryObject(5, 1, "sub-object-2", mockObjectStore)
    entitiesTree.add(bucket)
    entitiesTree.add(container, bucket.id)
    entitiesTree.add(container2, container.id)
    entitiesTree.add(memObj, container.id)
    entitiesTree.add(memObj2, container2.id)

    val entities = entitiesTree.listObjects(1, recursive = false)

    assertResult(Set.empty[MemoryObject])(entities)
  }

  test("isBucket for sub container results with false") {

    val bucket = Container(1, -1, "bucket", mockObjectStore)
    val container = Container(2, 1, "sub-container", mockObjectStore)
    entitiesTree.add(bucket)
    entitiesTree.add(container, bucket.id)

    val result = entitiesTree.isBucket(container.id)

    assert(!result)
  }

  test("isBucket for object results with false") {

    val bucket = Container(1, -1, "bucket", mockObjectStore)
    val container = Container(2, 1, "sub-container", mockObjectStore)
    val memObj = MemoryObject(3, 1, "sub-object-1", mockObjectStore)
    entitiesTree.add(bucket)
    entitiesTree.add(container, bucket.id)
    entitiesTree.add(memObj, container.id)

    val result = entitiesTree.isBucket(memObj.id)

    assert(!result)
  }

  test("isBucket for top container results with true") {

    val bucket = Container(1, -1, "bucket", mockObjectStore)
    val container = Container(2, 1, "sub-container", mockObjectStore)
    entitiesTree.add(bucket)
    entitiesTree.add(container, bucket.id)

    val result = entitiesTree.isBucket(bucket.id)

    assert(result)
  }

  test("delete empty container") {

    val bucket = Container(1, -1, "bucket", mockObjectStore)
    val container = Container(2, 1, "sub-container", mockObjectStore)
    entitiesTree.add(bucket)
    entitiesTree.add(container, bucket.id)

    entitiesTree.delete(container.id, bucket.id)
    val result = entitiesTree.get(container.id)

    assertResult(None)(result)
  }

  test("delete non empty container results with exception") {

    val bucket = Container(1, -1, "bucket", mockObjectStore)
    val container = Container(2, 1, "sub-container", mockObjectStore)
    val memObj = MemoryObject(3, 1, "sub-object-1", mockObjectStore)
    entitiesTree.add(bucket)
    entitiesTree.add(container, bucket.id)
    entitiesTree.add(memObj, container.id)

    intercept[IllegalStateException] {
      entitiesTree.delete(container.id, bucket.id)
    }
  }

  test("delete object") {

    val bucket = Container(1, -1, "bucket", mockObjectStore)
    val container = Container(2, 1, "sub-container", mockObjectStore)
    val memObj = MemoryObject(3, 1, "sub-object-1", mockObjectStore)
    entitiesTree.add(bucket)
    entitiesTree.add(container, bucket.id)
    entitiesTree.add(memObj, container.id)

    entitiesTree.delete(memObj.id, container.id)
    val result = entitiesTree.get(memObj.id)

    assertResult(None)(result)
  }

  test("delete container when parent does not exits does nothing") {

    entitiesTree.delete(10, 2)
  }

  test("hasChildEntries for empty bucket results with false") {

    val bucket = Container(1, -1, "bucket", mockObjectStore)
    entitiesTree.add(bucket)

    val hasChildren = entitiesTree.hasChildEntries(bucket.id)

    assert(!hasChildren)
  }

  test("hasChildEntries for non empty bucket results with true") {

    val bucket = Container(1, -1, "bucket", mockObjectStore)
    val container = Container(2, 1, "sub-container", mockObjectStore)
    entitiesTree.add(bucket)
    entitiesTree.add(container, bucket.id)

    val hasChildren = entitiesTree.hasChildEntries(bucket.id)

    assert(hasChildren)
  }

  test("hasChildEntries for empty container results with false") {

    val bucket = Container(1, -1, "bucket", mockObjectStore)
    val container = Container(2, 1, "sub-container", mockObjectStore)
    entitiesTree.add(bucket)
    entitiesTree.add(container, bucket.id)

    val hasChildren = entitiesTree.hasChildEntries(container.id)

    assert(!hasChildren)
  }

  test("hasChildEntries for non empty container results with true") {

    val bucket = Container(1, -1, "bucket", mockObjectStore)
    val container = Container(2, 1, "sub-container", mockObjectStore)
    val memObj = MemoryObject(3, 1, "sub-object-1", mockObjectStore)
    entitiesTree.add(bucket)
    entitiesTree.add(container, bucket.id)
    entitiesTree.add(memObj, container.id)

    val hasChildren = entitiesTree.hasChildEntries(container.id)

    assert(hasChildren)
  }

  test("hasChildEntries for object results with false") {

    val bucket = Container(1, -1, "bucket", mockObjectStore)
    val container = Container(2, 1, "sub-container", mockObjectStore)
    val memObj = MemoryObject(3, 1, "sub-object-1", mockObjectStore)
    entitiesTree.add(bucket)
    entitiesTree.add(container, bucket.id)
    entitiesTree.add(memObj, container.id)

    val hasChildren = entitiesTree.hasChildEntries(memObj.id)

    assert(!hasChildren)
  }

  test("exist by id for non exist item results with false") {

    val exist = entitiesTree.exist(1)

    assert(!exist)
  }

  test("exist by id for exist item results with true") {

    val bucket = Container(1, -1, "bucket", mockObjectStore)
    val container = Container(2, 1, "sub-container", mockObjectStore)
    val memObj = MemoryObject(3, 1, "sub-object-1", mockObjectStore)
    entitiesTree.add(bucket)
    entitiesTree.add(container, bucket.id)
    entitiesTree.add(memObj, container.id)

    val exist = entitiesTree.exist(3)

    assert(exist)
  }

  test("exist by name and parent for non exist item results with false") {

    val bucket = Container(1, -1, "bucket", mockObjectStore)
    val container = Container(2, 1, "sub-container", mockObjectStore)
    val memObj = MemoryObject(3, 1, "sub-object-1", mockObjectStore)
    entitiesTree.add(bucket)
    entitiesTree.add(container, bucket.id)
    entitiesTree.add(memObj, container.id)

    val exist = entitiesTree.exist("sub-container-x", bucket.id)

    assert(!exist)
  }

  test("exist by name and parent for exist item results with true") {

    val bucket = Container(1, -1, "bucket", mockObjectStore)
    val container = Container(2, 1, "sub-container", mockObjectStore)
    val memObj = MemoryObject(3, 1, "sub-object-1", mockObjectStore)
    entitiesTree.add(bucket)
    entitiesTree.add(container, bucket.id)
    entitiesTree.add(memObj, container.id)

    val exist = entitiesTree.exist("sub-container", bucket.id)

    assert(exist)
  }
}
