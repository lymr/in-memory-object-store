package com.lymr.memos

import org.scalatest.FunSuite

import InMemoryObjectStoreSuite._

class InMemoryObjectStoreSuite extends FunSuite {

  test("memory store example") {

    val memos = InMemoryObjectStore(STORE_CAPACITY, STORE_BLOCK_SIZE)
    assertResult(STORE_CAPACITY)(memos.memoryTotalCapacity)

    val bucket = memos.createBucket(BUCKET)
    val container1 = memos.createContainer(CONTAINER_1, bucket)
    val container2 = memos.createContainer(CONTAINER_2, container1)
    val object1 = memos.createObject(OBJECT_NAME_1, OBJECT_CONTENT_1, container1)
    val object2 = memos.createObject(OBJECT_NAME_2, OBJECT_CONTENT_2, container1)
    val object3 = memos.createObject(OBJECT_NAME_3, OBJECT_CONTENT_3, container2)

    val listBucket = memos.listObjects(bucket, recursive = true)
    assertResult(Set(object1, object2, object3))(listBucket)

    val object3Content = memos.read(object3).allStrict()
    assertResult(OBJECT_CONTENT_3)(object3Content)

    val object1Content = memos.read(object1).all().toArray
    assertResult(OBJECT_CONTENT_1)(object1Content)

    memos.updateObject(object1, ADDITINAL_CONTENT, append = true)
    val object1UpdatedContent = memos.read(object1).allStrict()
    assertResult(OBJECT_CONTENT_1 ++ ADDITINAL_CONTENT)(object1UpdatedContent)

    memos.deleteObject(object1)
    val listUpdatedBucket = memos.listObjects(bucket, recursive = true)
    assertResult(Set(object2, object3))(listUpdatedBucket)

    memos.deleteContainer(bucket, recursive = true)
    assertResult(ZERO)(memos.memoryUsed)
    assertResult(STORE_CAPACITY)(memos.memoryAvailable)
  }
}

object InMemoryObjectStoreSuite {
  val ZERO = 0
  val STORE_CAPACITY = 2048
  val STORE_BLOCK_SIZE = 128

  val BUCKET = "bucket-1"
  val CONTAINER_1 = "container-1"
  val CONTAINER_2 = "container-2"
  val OBJECT_NAME_1 = "test-object-1"
  val OBJECT_NAME_2 = "test-object-2"
  val OBJECT_NAME_3 = "test-object-3"
  val OBJECT_CONTENT_1 = "this-is-a-test-run-object-1".getBytes
  val OBJECT_CONTENT_2 = "this-is-a-test-run-object-2-a-b-c-d-e-f-g-h".getBytes
  val OBJECT_CONTENT_3 = ("Lorem Ipsum is simply dummy text of the printing and typesetting industry." +
    " Lorem Ipsum has been the industry's standard dummy text ever since the 1500s," +
    " when an unknown printer took a galley of type and scrambled it to make a type" +
    " specimen book. It has survived not only five centuries, but also the leap into" +
    " electronic typesetting, remaining essentially unchanged. It was popularised in" +
    " the 1960s with the release of Letraset sheets containing Lorem Ipsum passages," +
    " and more recently with desktop publishing software like Aldus PageMaker including" +
    " versions of Lorem Ipsum.").getBytes
  val ADDITINAL_CONTENT = "-additional-content-12345".getBytes
}
