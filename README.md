# In-Memory Object Store
An In-Memory Object Store in Scala 

[![Build Status](https://travis-ci.org/lymr/in-memory-object-store.svg?branch=master)](https://travis-ci.org/lymr/in-memory-object-store)

## Design Considerations
An in memory storage with fixed capacity and block size (configurable on initialization), each object in the store is
composed by a 2D resizable array of bytes where the first dimension has a fixed size (the block size).

The main motivation behind slicing each object into blocks:
- Amplified performance for append operations, only newly appended data is copied at object's tail.
- Allocating new blocks only when new data length is greater than available space at object's last block, 
in case when new appended content fits the available space in last block it just being copy to that space 
otherwise additional blocks are allocated and added to current space tail.
- Amortized performance of sequential read, as read operation on RAM are significantly faster than random reads.
- Additional memory can be allocated as long as there is available memory in a block size.
- Deallocate unused space with no need to copy data.

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