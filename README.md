# In-Memory Objects Store
An In-Memory Object Store in Scala 

[![Build Status](https://travis-ci.org/lymr/in-memory-object-store.svg?branch=master)](https://travis-ci.org/lymr/in-memory-object-store)

## Execution:
Compile project: `sbt compile`

Test project : `sbt test`

## Example 
```scala
import com.lymr.memos.InMemoryObjectStore

val memos = InMemoryObjectStore(capacity = 2048, blockSize = 256)

val bucket = memos.createBucket("my-bucket")
val container1 = memos.createContainer("container-1", bucket)
val container2 = memos.createContainer("sub-container-2", container1)
val object1 = memos.createObject("object-1", "some-content".getBytes, container1)
val object2 = memos.createObject("object-2", "another-object-content".getBytes, container1)
val object3 = memos.createObject("object-3", "some very long content ...".getBytes, container2)

val listBucket = memos.listObjects(bucket, recursive = true)
assertResult(Set(object1, object2, object3))(listBucket)

val object3Content = memos.read(object3).allStrict()
assertResult("some very long content ...".getBytes)(object3Content)

val object1Content = memos.read(object1).all().toArray
assertResult("some-content".getBytes)(object1Content)

memos.updateObject(object1, "-additional-content".getBytes, append = true)
val object1UpdatedContent = memos.read(object1).allStrict()
assertResult("some-content-additional-content".getBytes)(object1UpdatedContent)

memos.deleteObject(object1)
val listUpdatedBucket = memos.listObjects(bucket, recursive = true)
assertResult(Set(object2, object3))(listUpdatedBucket)

memos.deleteContainer(bucket, recursive = true)
assertResult(0)(memos.memoryUsed)
assertResult(2048)(memos.memoryAvailable)
```

## Copyright
Copyright (c) 2018 Mor Levy