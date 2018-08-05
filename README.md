#In-Memory Objects Store
An In-Memory Object Store in Scala

##Execution:
Compile project: `sbt compile`

Test project : `sbt test`

##Example 
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

##License
MIT License

Copyright (c) 2018 Mor Levy

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.