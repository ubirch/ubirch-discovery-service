package com.ubirch.discovery.kafka.models

import java.util.concurrent.CountDownLatch

import com.typesafe.scalalogging.LazyLogging

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.{ ExecutionContext, Future }
import scala.util.Try

/**
  * Class that will execute a function f on all the objects in objects asynchronously, but only processing processSize
  * at the same time.
  * This is helpful when working against a resource that has a limited amount of processing power.
  * For example, when working against a gremlin-server, the processSize should be equal to its amount of workers
  *
  * @param objects The objects to process, accompanied with the function f() that will process them
  * @param processSize How many time f() will be executed at the same time in parallel
  * @param ec The execution context
  * @tparam T Original type of the item in object
  * @tparam U What type of objects f() will transform the objects
  */
class Executor[T, U](objects: Seq[T], f: T => U, val processSize: Int, customResultFunction: Option[() => Unit] = None)(implicit ec: ExecutionContext) extends LazyLogging {

  /**
    * The original list of objects on which the f() function will be applied.
    * For ease of coding, they've been transformed into a HashMap
    */
  private val toBeProcessedList: mutable.HashMap[Int, T] = scala.collection.mutable.HashMap.empty[Int, T]

  /**
    * The executionList is a mutable HashMap whose size can vary between 0 and processSize.
    * This is where the work is happening.
    * Each object in the execution list is added with addToExecutionList. This mean that each object is
    * a Future of a function applied to an object taken from the toBeProcessedList list.
    * Once the future is complete, the object will be removed from the executionList, put in the result list,
    * and a new one, if available, will take his place.
    */
  private val executionList: mutable.HashMap[Int, Future[U]] = scala.collection.mutable.HashMap.empty[Int, Future[U]]

  /**
    * Once the objects have been processed by f(), their result (or failure) is stored in the results object, alongside
    * the initial object
    */
  private val results: ArrayBuffer[(T, Try[U])] = scala.collection.mutable.ArrayBuffer.empty[(T, Try[U])]

  /**
    * A countdown latch that will be decreased once all objects are processed
    */
  val latch = new CountDownLatch(1)

  def startProcessing(): Unit = {
    objectsToMap()
    fillUpExecutionList()
  }

  def getResults: List[(T, Try[U])] = results.toList
  def getResultsNoTry: List[(T, U)] = results.map { t => (t._1, t._2.get) }.toList
  def getResultsOnlySuccess: List[(T, U)] = results.filter { t => t._2.isSuccess }.map { t => (t._1, t._2.get) }.toList

  /**
    * Add item to the execution list until
    * - The execution list size is equal to processSize OR
    * - There is no more object to be processed
    */
  private def fillUpExecutionList(): Unit = {
    while (!isExecutionListFull & toBeProcessedList.nonEmpty) {
      addNewItemToExecutionList()
    }
  }

  /**
    * Check is all objects have been treated
    */
  def isOver: Boolean = executionList.isEmpty

  private def objectsToMap(): Unit = {
    for (i <- objects.indices) {
      toBeProcessedList += (i -> objects(i))
    }
  }

  /**
    * Add a new item to the execution list.
    * This item will be a Future of f(item).
    * Once the computation of f(item) is over, it'll try to add a new item to the execution list
    * @param thingToAdd
    * @return
    */
  private def addToExecutionList(thingToAdd: (Int, T)) = {

    val ourFuture: Future[U] = Future(f(thingToAdd._2)) andThen {
      case resOfFunction: Try[U] =>
        callCustomCallBackFunctionIfExist()
        removeFromExecutionList(thingToAdd._1)
        addToResults(thingToAdd._2, resOfFunction)
        addNewItemToExecutionList()
        checkIfOver()
    }
    executionList ++= Map(thingToAdd._1 -> ourFuture)
  }

  /*
  Execute the custom result function passed as argument (if any)
  Can be used to increase prometheus counter, for example
   */
  private def callCustomCallBackFunctionIfExist(): Unit = {
    customResultFunction match {
      case Some(function) => function()
      case None =>
    }
  }

  private def checkIfOver(): Unit = {
    if (isOver) latch.countDown()
  }

  private def isExecutionListFull: Boolean = executionList.size == processSize

  private def addNewItemToExecutionList(): Unit = synchronized {
    if (toBeProcessedList.nonEmpty) {
      val toProcess = toBeProcessedList.head
      toBeProcessedList.remove(toProcess._1)
      addToExecutionList(toProcess)
    }
  }

  private def removeFromExecutionList(finished: Int) = synchronized {
    executionList.remove(finished)
  }

  private def addToResults(finished: (T, Try[U])) = synchronized {
    results += finished
  }

}
