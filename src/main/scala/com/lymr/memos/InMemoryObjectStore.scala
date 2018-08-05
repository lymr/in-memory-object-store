package com.lymr.memos

import java.util.concurrent.atomic.AtomicInteger

import com.lymr.memos.entities._
import com.lymr.memos.storage._
import com.lymr.memos.structure.StoreEntitiesTree

/** An in memory RAM object store composed of fixed size blocks of bytes.
  * The store has a pre-defined capacity and block size.
  *
  * @param storeStructure Underlying objects's data structure
  * @param objectStorage  Underlying binary object storage
  */
class InMemoryObjectStore private (storeStructure: StoreEntitiesTree, objectStorage: InMemoryObjectStorage) {

  // --- Private Members --- //

  /** Entity ID generator */
  private val idGenerator = new AtomicInteger(1)

  // --- Public Methods --- //

  /** Creates an empty [[MemoryObject]] under the given parent [[Container]]
    *
    * @param name      The name of the object to be created
    * @param container Parent [[Container]]
    * @throws IllegalStateException    In case when another store entity exists with the same name under the parent [[Container]]
    * @throws IllegalArgumentException In case when given object name equals null or an empty string.
    *                                  In case wen parent [[Container]] is null
    * @return A newly created object
    */
  @throws[IllegalStateException]
  @throws[IllegalArgumentException]
  def createObject(name: String, container: Container): MemoryObject = {
    require(name != null && name.length > 0, "object name must not be Null nor empty")
    require(container != null, "container must not be null")

    if (storeStructure.exist(name, container.id))
      throw new IllegalStateException(s"name:='$name' already exist")

    val obj = MemoryObject(idGenerator.getAndIncrement(), container.id, name, this)
    objectStorage.add(obj.id)
    container.updateModificationTime()
    storeStructure.add(obj, container.id)
    obj
  }

  /** Creates an [[MemoryObject]] with raw binary content under the given parent [[Container]]
    *
    * @param name      The name of the object to be created
    * @param content   Raw binary content to write to object
    * @param container Parent [[Container]]
    * @throws IllegalStateException    In case when another store entity exists with the same name under the parent [[Container]]
    * @throws IllegalArgumentException In case when given object name equals null or an empty string
    *                                  In case wen parent [[Container]] is null
    * @throws OutOfDiskMemoryException In case when underlying [[InMemoryDisk]] is unable to allocate enough space
    *                                  for this object
    * @return A newly created object with content
    */
  @throws[IllegalStateException]
  @throws[IllegalArgumentException]
  @throws[OutOfDiskMemoryException]
  def createObject(name: String, content: Array[Byte], container: Container): MemoryObject = {
    require(name != null && name.length > 0, "object name must not be Null nor empty")
    require(container != null, "container must not be null")

    if (storeStructure.exist(name, container.id))
      throw new IllegalStateException(s"name:='$name' already exist")

    val obj = MemoryObject(idGenerator.getAndIncrement(), container.id, name, this)
    objectStorage.write(obj.id, content, append = false)
    container.updateModificationTime()
    storeStructure.add(obj, container.id)
    obj
  }

  /** Creates a bucket (root container)
    *
    * @param name Bucket name
    * @throws IllegalStateException    In case when bucket exists with the same name
    * @throws IllegalArgumentException In case when given bucket name equals null or an empty string
    * @return A newly created bucket (root container)
    */
  @throws[IllegalStateException]
  @throws[IllegalArgumentException]
  def createBucket(name: String): Container = {
    require(name != null && name.length > 0, "bucket name must not be Null nor empty")

    if (storeStructure.exist(name, -1))
      throw new IllegalStateException(s"name:='$name' already exist")

    val container = Container(idGenerator.getAndIncrement(), -1, name, this)
    storeStructure.add(container)
    container
  }

  /** Creates a [[Container]] under the given parent [[Container]]
    *
    * @param name            The name of the container to be created
    * @param parentContainer Parent [[Container]]
    * @throws IllegalStateException    In case when another store entity exists with the same name under the parent [[Container]]
    * @throws IllegalArgumentException In case when given container name equals null or an empty string
    *                                  In case wen parent [[Container]] is null
    * @return A newly created container
    */
  @throws[IllegalStateException]
  @throws[IllegalArgumentException]
  def createContainer(name: String, parentContainer: Container): Container = {
    require(name != null && name.length > 0, "container name must not be Null nor empty")
    require(parentContainer != null, "parent container must not be null")

    if (storeStructure.exist(name, parentContainer.id))
      throw new IllegalStateException(s"name:='$name' already exist")

    val container = Container(idGenerator.getAndIncrement(), parentContainer.id, name, this)
    storeStructure.add(container, parentContainer.id)
    container
  }

  /** Reads an [[MemoryObject]] content
    *
    * @param obj An object to read
    * @return A string or Raw binary representation of object's content
    */
  def read(obj: MemoryObject) = new {

    def objectNotFound = {
      throw new IllegalStateException(s"object name:='${obj.name}' not found")
    }

    def all(): Iterator[Byte] = objectStorage.read(obj.id).getOrElse(objectNotFound)

    def from(offset: Int, n: Int): Iterator[Byte] = objectStorage.read(obj.id, offset, n).getOrElse(objectNotFound)

    def allStrict(): Array[Byte] = objectStorage.readStrict(obj.id).getOrElse(objectNotFound)

    def fromStrict(offset: Int, n: Int): Array[Byte] = objectStorage.readStrict(obj.id).getOrElse(objectNotFound)
  }

  /** Updates object's content
    *
    * @param obj     Given object to update
    * @param content Binary content to update
    * @param append  Whether to append or override objects's existing content
    * @throws OutOfDiskMemoryException In case when underlying [[InMemoryDisk]] is unable to allocate enough space
    *                                  for this object
    */
  @throws[OutOfDiskMemoryException]
  def updateObject(obj: MemoryObject, content: Array[Byte], append: Boolean): Unit = {
    objectStorage.write(obj.id, content, append)
  }

  /** Lists the all store entities under the given container
    *
    * @param container The given [[Container]] to list
    * @param recursive Whether to traverse the container recursively
    * @return A `Set` of all sub store entities
    */
  def list(container: Container, recursive: Boolean = false): Set[StoreEntity] = {
    container.updateAccessTime()
    list(container.id, recursive)
  }

  /** Lists the all objects under the given container
    *
    * @param container The given [[Container]] to list
    * @param recursive Whether to traverse the container recursively
    * @return A `Set` of all sub store objects
    */
  def listObjects(container: Container, recursive: Boolean = false): Set[MemoryObject] = {
    container.updateAccessTime()
    listObjects(container.id, recursive)
  }

  /** Deletes an [[MemoryObject]] from the given store */
  def deleteObject(obj: MemoryObject): Unit = {
    obj.parent.foreach(_.updateModificationTime())
    delete(obj.id, recursive = false)
  }

  /** Deletes a [[Container]] from the given store
    *
    * @param container Given [[Container]] to delete
    * @param recursive Whether to delete the container recursively
    */
  def deleteContainer(container: Container, recursive: Boolean = false): Unit = {
    container.parent.foreach(_.updateModificationTime())
    delete(container.id, recursive)
  }

  /** Object store used memory in bytes */
  def memoryUsed: Int = objectStorage.memoryUsed

  /** Object store available memory in bytes */
  def memoryAvailable: Int = objectStorage.memoryAvailable

  /** Object store total memory in bytes */
  def memoryTotalCapacity: Int = objectStorage.memoryTotalCapacity

  // --- Package Private --- //

  private[memos] def findParent(parentId: Int): Option[Container] = {
    storeStructure.get(parentId).collect { case container: Container => container }
  }

  private[memos] def delete(id: Int, recursive: Boolean): Unit = {

    storeStructure.get(id).foreach {
      case obj: MemoryObject =>
        objectStorage.remove(obj.id)
        storeStructure.delete(id, obj.parentId)

      case cnt: Container if recursive =>
        list(cnt.id, recursive = false).foreach(e => delete(e.id, recursive = true))
        storeStructure.delete(id, cnt.parentId)

      case cnt: Container =>
        storeStructure.delete(id, cnt.parentId)
    }
  }

  private[memos] def list(id: Int, recursive: Boolean): Set[StoreEntity] = {
    storeStructure.list(id, recursive)
  }

  private[memos] def listObjects(id: Int, recursive: Boolean): Set[MemoryObject] = {
    storeStructure.listObjects(id, recursive)
  }

  private[memos] def size(id: Int): Int = {

    storeStructure.get(id) match {
      case Some(obj: MemoryObject) =>
        objectStorage.objectSize(obj.id).getOrElse(0)

      case Some(cnt: Container) =>
        listObjects(cnt.id, recursive = true).flatMap(obj => objectStorage.objectSize(obj.id)).sum

      case None => 0
    }
  }

  private[memos] def sizeOnDisk(id: Int): Int = {

    storeStructure.get(id) match {
      case Some(obj: MemoryObject) =>
        objectStorage.objectSizeOnDisk(obj.id).getOrElse(0)

      case Some(cnt: Container) =>
        listObjects(cnt.id, recursive = true).flatMap(obj => objectStorage.objectSizeOnDisk(obj.id)).sum

      case None => 0
    }
  }

  private[memos] def isBucket(id: Int): Boolean = {
    storeStructure.isBucket(id)
  }

  private[memos] def createTime(id: Int): Long = {
    objectStorage.readMetadata(id).map(_.createTime).getOrElse(-1)
  }

  private[memos] def lastAccessTime(id: Int): Long = {
    objectStorage.readMetadata(id).map(_.lastAccessTime).getOrElse(-1)
  }

  private[memos] def lastModificationTime(id: Int): Long = {
    objectStorage.readMetadata(id).map(_.lastModificationTime).getOrElse(-1)
  }
}

object InMemoryObjectStore {

  def apply(capacity: Int, blockSize: Int = Configuration.defaultBlockSize): InMemoryObjectStore = {
    val disk = InMemoryDisk.withCapacity(capacity, blockSize)
    new InMemoryObjectStore(new StoreEntitiesTree, new InMemoryObjectStorage(disk))
  }
}
