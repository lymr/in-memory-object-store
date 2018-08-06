package com.lymr.memos.storage

import com.lymr.memos.storage.InMemoryObjectStorage._

import scala.collection.mutable

/** In memory object storage composite */
private[memos] class InMemoryObjectStorage(disk: InMemoryDisk) {

  // -- Private Members --- //

  private val storage = mutable.Map[Int, StoreObject]()

  // -- Public Methods --- //

  /** Reads object's metadata */
  def readMetadata(id: Int): Option[ObjectMetadata] = storage.get(id).map(_.meta)

  /** Reads object content */
  def read(id: Int): Option[Iterator[Byte]] = fetchObject(id)(_.read())

  /** Reads `n` bytes from object content */
  def read(id: Int, offset: Int, n: Int): Option[Iterator[Byte]] = fetchObject(id)(_.read(offset, n))

  /** Reads object content strictly */
  def readStrict(id: Int): Option[Array[Byte]] = fetchObject(id)(_.readStrict())

  /** Reads `n` bytes from object content strictly */
  def readStrict(id: Int, offset: Int, n: Int): Option[Array[Byte]] = fetchObject(id)(_.readStrict(offset, n))

  /** Adds an empty object to storage */
  def add(id: Int): Unit = storage.get(id) match {
    case Some(_) => throw new IllegalStateException(s"object with id:= $id already exist!")
    case None    => storage.put(id, StoreObject.createEmpty(disk))
  }

  /** Write content to an existing object or creates a new object if needed.
    *
    * @param id      The Id of the associated object
    * @param content The content to write
    * @param append  Whether to append or override object's content with the given content
    */
  def write(id: Int, content: Array[Byte], append: Boolean): Unit = {
    val updatedObjectContent = storage.get(id) match {
      case Some(objectContent) =>
        objectContent.updateContent(content, append).updateMetadata(_.updateModifyTime())
      case None =>
        StoreObject.createContent(content, disk)
    }
    storage.put(id, updatedObjectContent)
  }

  /** Object's actual size */
  def objectSize(id: Int): Option[Int] = fetchObject(id)(_.size)

  /** Object's size on disk */
  def objectSizeOnDisk(id: Int): Option[Int] = fetchObject(id)(_.totalSize)

  /** Removes object's with given id content and metadata */
  def remove(id: Int): Unit = storage.remove(id).foreach(_.raw.clear())

  /** Store used memory in bytes */
  def memoryUsed: Int = disk.used

  /** Store available memory in bytes */
  def memoryAvailable: Int = disk.available

  /** Store total memory in bytes */
  def memoryTotalCapacity: Int = disk.totalCapacity

  // --- Private Methods --- //

  private def fetchObject[A](id: Int)(extract: InMemoryBlob => A): Option[A] = {
    storage.get(id).map { obj =>
      val updatedObject = obj.updateMetadata(_.updateAccessTime())
      storage.put(id, updatedObject)
      extract(updatedObject.raw)
    }
  }
}

object InMemoryObjectStorage {

  // --- Inner Classes --- //

  /** In Memory object metadata */
  final case class ObjectMetadata(createTime: Long, lastAccessTime: Long, lastModificationTime: Long) {

    def updateAccessTime(time: Long = System.currentTimeMillis): ObjectMetadata = {
      this.copy(lastAccessTime = time)
    }

    def updateModifyTime(time: Long = System.currentTimeMillis): ObjectMetadata = {
      this.copy(lastModificationTime = time)
    }
  }

  private object ObjectMetadata {

    def create(time: Long = System.currentTimeMillis): ObjectMetadata = {
      ObjectMetadata(time, time, time)
    }
  }

  /** Holds Objects's metadata and actual data */
  private case class StoreObject(meta: ObjectMetadata, raw: InMemoryBlob) {

    def updateMetadata(obj: ObjectMetadata => ObjectMetadata): StoreObject = {
      this.copy(meta = obj(meta))
    }

    def updateContent(content: Array[Byte], append: Boolean): StoreObject = {
      this.copy(raw = raw.write(content, append))
    }
  }

  private object StoreObject {

    def createEmpty(disk: InMemoryDisk): StoreObject = {
      StoreObject(ObjectMetadata.create(), InMemoryBlob.empty(disk))
    }

    def createContent(content: Array[Byte], disk: InMemoryDisk): StoreObject = {
      StoreObject(ObjectMetadata.create(), InMemoryBlob.withContent(content, disk))
    }
  }
}
