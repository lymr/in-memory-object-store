package com.lymr.memos.entities

import com.lymr.memos.InMemoryObjectStore

/** An in-memory store entity representation with common properties */
sealed trait StoreEntity {

  /** A unique identifier of the given store entity */
  def id: Int

  /** The name of the given store entity */
  def name: String

  /** The given store entity creation timestamp since Epoch */
  def createTime: Long

  /** The given store entity last access timestamp since Epoch */
  def lastAccessTime: Long

  /** The given store entity last modification timestamp since Epoch */
  def lastModificationTime: Long

  /** True whether the store entity is an object, otherwise False */
  def isObject: Boolean

  /** True whether the store entity is a container, otherwise False */
  def isContainer: Boolean

  /** True whether the store entity is a bucket, otherwise False */
  def isBucket: Boolean

  /** Actual size of the given store entity */
  def size: Int

  /** Disk size of the given store entity */
  def sizeOnDisk: Int

  /** An optional value representing the parent [[Container]] of the actual [[StoreEntity]], in case
    * when the parent [[Container]] does not exist the method will return `None`.
    */
  def parent: Option[Container]
}

/** An immutable representation of an Object within the [[InMemoryObjectStore]].
  *
  * @param id       The id of the given object within the [[InMemoryObjectStore]]
  * @param parentId The given object parent [[Container]] id within the [[InMemoryObjectStore]]
  * @param name     The name of the given [[MemoryObject]]
  * @param os       The underlying [[InMemoryObjectStore]]
  */
final case class MemoryObject(id: Int, parentId: Int, name: String, private val os: InMemoryObjectStore)
    extends StoreEntity {

  // --- Public Methods --- //

  override def createTime: Long = os.createTime(id)

  override def lastAccessTime: Long = os.lastAccessTime(id)

  override def lastModificationTime: Long = os.lastModificationTime(id)

  override def isObject: Boolean = true

  override def isContainer: Boolean = false

  override def isBucket: Boolean = os.isBucket(id)

  override def size: Int = os.size(id)

  override def sizeOnDisk: Int = os.sizeOnDisk(id)

  override def parent: Option[Container] = os.findParent(parentId)
}

/** An immutable representation of a container within the [[InMemoryObjectStore]].
  *
  * @param id       The id of the given container within the [[InMemoryObjectStore]]
  * @param parentId The given container parent [[Container]] id within the [[InMemoryObjectStore]]
  * @param name     The name of the given [[Container]]
  * @param os       The underlying [[InMemoryObjectStore]]
  */
final case class Container(id: Int, parentId: Int, name: String, private val os: InMemoryObjectStore)
    extends StoreEntity {

  // --- Private Members --- //

  private val _createTime: Long = System.currentTimeMillis

  private var _lastAccessTime: Long = System.currentTimeMillis

  private var _lastModificationTime: Long = System.currentTimeMillis

  // --- Public Methods --- //

  override def createTime: Long = _createTime

  override def lastAccessTime: Long = _lastAccessTime

  override def lastModificationTime: Long = _lastModificationTime

  override def isObject: Boolean = false

  override def isContainer: Boolean = true

  override def isBucket: Boolean = os.isBucket(id)

  override def size: Int = os.size(id)

  override def sizeOnDisk: Int = os.sizeOnDisk(id)

  override def parent: Option[Container] = os.findParent(parentId)

  // --- Package Private --- //

  private[memos] def updateAccessTime(time: Long = System.currentTimeMillis): Unit = {
    _lastAccessTime = time
  }

  private[memos] def updateModificationTime(time: Long = System.currentTimeMillis): Unit = {
    _lastModificationTime = time
  }
}
