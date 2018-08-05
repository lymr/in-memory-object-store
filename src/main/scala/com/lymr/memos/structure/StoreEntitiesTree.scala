package com.lymr.memos.structure

import com.lymr.memos.entities._

import scala.collection.mutable

/** Object store data tree implementation using HashMap's to represent relationships between an
  * inner node ([[Container]]) to its child nodes ([[Container]] or [[MemoryObject]]).
  */
final class StoreEntitiesTree() {

  // --- Private Members --- //

  /** Root containers */
  private val buckets = mutable.Map[Int, Container]()

  /** A mapping between store-entity ID to its associated Entity */
  private val storeEntities = mutable.Map[Int, StoreEntity]()

  /** A mapping between a container ID to all of its direct sub store entries (containers / objects) */
  private val containerEntries = mutable.Map[Int, mutable.Set[Int]]()

  // --- Public Methods --- //

  /** Add a root container (bucket) */
  def add(container: Container): Unit = {
    buckets.put(container.id, container)
    storeEntities.put(container.id, container)
  }

  /** Add a sub container under a bucket or other container */
  def add(entity: StoreEntity, parentId: Int): Unit = {
    if (exist(entity.id))
      throw new IllegalStateException(s"entity with id:'${entity.id}' already exist")

    if (!exist(parentId))
      throw new IllegalStateException(s"parent container with id:'$parentId' does not exist")

    containerEntries.getOrElseUpdate(parentId, mutable.Set[Int]()).add(entity.id)
    storeEntities.put(entity.id, entity)
  }

  /** Fetch a store entity */
  def get(id: Int): Option[StoreEntity] = storeEntities.get(id)

  /** List all store buckets */
  def listBuckets(): Set[Container] = buckets.values.toSet

  /** List all store entities under the given id */
  def list(id: Int, recursive: Boolean): Set[StoreEntity] = {

    containerEntries.get(id) match {

      case Some(subEntries) if recursive =>
        val (objects, containers) = subEntries.flatMap(storeEntities.get).partition(e => e.isObject)
        (objects ++ containers).toSet ++ containers.flatMap(e => list(e.id, recursive))

      case Some(subEntries) =>
        subEntries.flatMap(storeEntities.get).toSet

      case None => Set.empty[StoreEntity]
    }
  }

  /** List all store objects under the given id */
  def listObjects(id: Int, recursive: Boolean): Set[MemoryObject] = {

    containerEntries.get(id) match {

      case Some(subEntries) if recursive =>
        val (objects, containers) = subEntries.flatMap(storeEntities.get).partition(e => e.isObject)
        objects.collect { case obj: MemoryObject => obj }.toSet ++ containers.flatMap(e => listObjects(e.id, recursive))

      case Some(subEntries) =>
        subEntries.flatMap(storeEntities.get).collect { case obj: MemoryObject => obj }.toSet

      case None => Set.empty[MemoryObject]
    }
  }

  /** Delete a store entity with given id, in case when the given id represents a container its must be empty.
    *
    * @param id       Given store entity to delete id
    * @param parentId Given store entity to delete parent container id
    */
  def delete(id: Int, parentId: Int): Unit = {
    if (hasChildEntries(id))
      throw new IllegalStateException("non-empty container")

    if (parentId == -1) buckets.remove(id)
    containerEntries.get(parentId).foreach(_.remove(id))
    containerEntries.remove(id)
    storeEntities.remove(id)
  }

  /** Whether the store entity with given id is bucket ot not */
  def isBucket(id: Int): Boolean = buckets.contains(id)

  def hasChildEntries(id: Int): Boolean = {
    containerEntries.get(id) match {
      case Some(subEntities) => subEntities.nonEmpty
      case None              => false
    }
  }

  /** Whether the store entity with given id exist or not */
  def exist(id: Int): Boolean = storeEntities.contains(id)

  /** Whether the store entity with given name and parent id exist or not */
  def exist(name: String, parentId: Int): Boolean = {
    containerEntries.get(parentId) match {
      case Some(subEntities) =>
        subEntities.flatMap(storeEntities.get).exists(e => e.name == name)
      case None => false
    }
  }
}
