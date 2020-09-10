package com.ubirch.discovery

import java.lang
import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.Condition

import com.google.inject.binder.ScopedBindingBuilder
import com.typesafe.config.{Config, ConfigValueFactory}
import com.ubirch.discovery.models.{EdgeCore, ElementProperty, Relation, VertexCore}
import com.ubirch.discovery.services.config.ConfigProvider
import com.ubirch.discovery.services.consumer.{AbstractDiscoveryService, DiscoveryApp}
import com.ubirch.discovery.util.Util
import com.ubirch.discovery.ExamplesUtil.FakeInjectorNoRedis
import com.ubirch.discovery.models.lock.Lock
import javax.inject.{Inject, Singleton}
import monix.execution.Scheduler
import org.redisson.api.{RFuture, RLock}

import scala.util.Random

object Launch {

  import Examples._

  def main(args: Array[String]): Unit = {
    generateSimpleGraph
    println("Done!")
  }
}

// create in two parts:
// 1: generate relations
// 2: store relations

object Examples {

  val Injector = FakeInjectorNoRedis(8182)
  val dc: AbstractDiscoveryService = Injector.get[AbstractDiscoveryService]
  import ExamplesUtil._


  def generateSimpleGraph = {
    val numberSt = scala.util.Random.nextInt(5) + 1
    val intervalFastBc = scala.util.Random.nextInt(4) + 1
    val intervalSlowBc = intervalFastBc + scala.util.Random.nextInt(4) + 1
    val numberMtUp = intervalSlowBc * 2 + scala.util.Random.nextInt(5) + 1
    val numberMtlow = intervalSlowBc * 2 + scala.util.Random.nextInt(5) + 1
    val slow_bc_label = "ETH"
    val fast_bc_label = "IOTA"
    val timestampInit = 100000
    val timestampIncrement = 1000
    var currentTimestamp = timestampInit
    var goingBackTimestamp = 0

    println(s"interval fast bc = $intervalFastBc")
    println(s"interval slow bc = $intervalSlowBc")

    def incrTimestamp = {
      currentTimestamp = incrementTimestamp(currentTimestamp, timestampIncrement)
    }

    def decrTimestamp = {
      goingBackTimestamp = decrementTimestamp(goingBackTimestamp, timestampIncrement)
    }

    val device = generateDevice(timestamp = currentTimestamp.toString)
    incrTimestamp
    val upp = generateUpp(timestamp = currentTimestamp.toString)
    val rUppDevice = relationUppDevice(upp, device, currentTimestamp.toString)
    incrTimestamp
    val firstSt = generateSlaveTree(timestamp = currentTimestamp.toString)
    val r1StUpp = relationFoundationUpp(firstSt, upp, currentTimestamp.toString)
    incrTimestamp

    def generateRelations(previousFT: VertexCore, counter: Int, accu: List[Relation], generateVertex: String => VertexCore, relation: (VertexCore, VertexCore, String) => Relation, normalOrder: Boolean = true): List[Relation] = {
      if (counter == 0) {
        accu
      } else {
        val ts = if (normalOrder) currentTimestamp else goingBackTimestamp
        val newSt = generateVertex(ts.toString)
        val newAccu = if (normalOrder) accu :+ relation(newSt, previousFT, ts.toString) else accu :+ relation(previousFT, newSt, ts.toString)
        if (normalOrder) incrTimestamp else decrTimestamp
        generateRelations(newSt, counter - 1, newAccu, generateVertex, relation, normalOrder)
      }
    }

    def stsFunction(timestamp: String) = generateSlaveTree(timestamp = timestamp)
    val sts = generateRelations(previousFT = firstSt, counter = numberSt, accu = Nil, stsFunction, relationFoundationFoundation)

    val firstMt = generateMasterTree(timestamp = currentTimestamp.toString)
    goingBackTimestamp = currentTimestamp - timestampIncrement
    val lastStFirstMt = relationMasterFoundation(firstMt, sts.last.vFrom, currentTimestamp.toString)
    incrTimestamp

    def mtsFunction(timestamp: String) = generateMasterTree(timestamp = timestamp)
    val mts = generateRelations(firstMt, counter = numberMtUp, accu = Nil, mtsFunction, relationMasterMaster)


    def addBc(bcLabel: String, mt: VertexCore) = {
      val ts = mt.properties.find(p => p.keyName == "timestamp").get.value.toString
      val bc = generateBlockchain(public_chain = bcLabel, timestamp = ts)
      relationBlockchainMaster(bc, mt, ts)
    }

    val maybeBcsUp = for (i <- 1 until numberMtUp) yield {
      val maybeSlow = if (i % intervalSlowBc == 0) {
        println(s"adding $slow_bc_label at $i")
        Some(addBc(slow_bc_label, mts(i).vTo))
      } else None
      val maybeFast = if (i % intervalFastBc == 0) {
        println(s"adding $fast_bc_label at $i")
        Some(addBc(fast_bc_label, mts(i).vTo))
      } else None
      (maybeSlow, maybeFast)
    }
    val bcsUp = maybeBcsUp.toList.flatMap(x => List(x._1, x._2)).flatten


    val lowerMts = generateRelations(firstMt, counter = numberMtlow, accu = Nil, mtsFunction, relationMasterMaster, normalOrder = false)

    val maybeBcsDown = for (i <- 1 until numberMtlow) yield {
      val maybeSlow = if (i % intervalSlowBc == 0) {
        println(s"adding $slow_bc_label at $i")
        Some(addBc(slow_bc_label, lowerMts(i).vTo))
      } else None
      val maybeFast = if (i % intervalFastBc == 0) {
        println(s"adding $fast_bc_label at $i")
        Some(addBc(fast_bc_label, lowerMts(i).vTo))
      } else None
      (maybeSlow, maybeFast)
    }

    val bcsDown = maybeBcsDown.toList.flatMap(x => List(x._1, x._2)).flatten

    val allRelations: List[Relation] = ((rUppDevice :: r1StUpp :: sts) :+ lastStFirstMt) ++ mts ++ bcsUp ++ lowerMts ++ bcsDown
    val storedRelations = storeRelations(dc, allRelations)
    println(s"made 1 device, 1 upp, ${sts.size + 1} foundation trees, ${mts.size} master trees up, 2 blockchains UP, back until 1st bc: ${numberMtlow}, 1 bc, ")
    storedRelations

    //    val st = generateSlaveTree()
//    val mt = generateMasterTree()
//    val blockchain = generateBlockchain()



  }

  def deviceToBLockchain = {
    val device = generateDevice()
    val upp = generateUpp()
    val upp2 = generateUpp()
    val st = generateSlaveTree()
    val mt = generateMasterTree()
    val blockchain = generateBlockchain()
    val relations = relationUppDevice(upp, device) :: relationUppDevice(upp2, device) :: relationFoundationUpp(st, upp) ::
      relationFoundationUpp(st, upp2) :: relationChain(upp, upp2) :: relationMasterFoundation(mt, st) ::
      relationBlockchainMaster(blockchain, mt) :: Nil
    storeRelations(dc, relations)
  }

  /**
  * Just a simple test to verify that the system is working. Creates a relation between a upp and a device
    */
  def generateFirstRelation = {
    val relations = relationUppDevice()
    storeRelations(dc, List(relations))
  }

}

object ExamplesUtil extends ExampleValues {

  def incrementTimestamp(current: Int, by: Int) = current + by

  def decrementTimestamp(current: Int, by: Int) = current - by

  def storeRelations(discoveryService: DiscoveryApp, relations: List[Relation]) = {
    discoveryService.store(relations)
  }

  def relationUppDevice(upp: VertexCore = generateUpp(), device: VertexCore = generateDevice(), timestamp: String = giveMeARandomTimestamp): Relation = {
    val edgeProps = Map("timestamp" -> timestamp)
    val edge = EdgeCore(generateElementPropertiesFromMap(edgeProps), EDGE_UPP_DEVICE)
    Relation(upp, device, edge)
  }

  def relationChain(uppFrom: VertexCore = generateUpp(), uppTo: VertexCore = generateUpp(), timestamp: String = giveMeARandomTimestamp): Relation = {
    val edgeProps = Map("timestamp" -> timestamp)
    val edge = EdgeCore(generateElementPropertiesFromMap(edgeProps), EDGE_CHAIN)
    Relation(uppFrom, uppTo, edge)
  }

  def relationFoundationUpp(foundation: VertexCore = generateSlaveTree(), upp: VertexCore = generateUpp(), timestamp: String = giveMeARandomTimestamp): Relation = {
    val edgeProps = Map("timestamp" -> timestamp)
    val edge = EdgeCore(generateElementPropertiesFromMap(edgeProps), EDGE_ST_UPP)
    Relation(foundation, upp, edge)
  }

  def relationFoundationFoundation(foundationFrom: VertexCore = generateSlaveTree(), foundationTo: VertexCore = generateSlaveTree(), timestamp: String = giveMeARandomTimestamp): Relation = {
    val edgeProps = Map("timestamp" -> timestamp)
    val edge = EdgeCore(generateElementPropertiesFromMap(edgeProps), EDGE_SLAVE_TREE_SLAVE_TREE)
    Relation(foundationFrom, foundationTo, edge)
  }

  def relationMasterFoundation(master: VertexCore = generateMasterTree(), foundation: VertexCore = generateSlaveTree(), timestamp: String = giveMeARandomTimestamp): Relation = {
    val edgeProps = Map("timestamp" -> timestamp)
    val edge = EdgeCore(generateElementPropertiesFromMap(edgeProps), EDGE_MASTER_TREE_SLAVE_TREE)
    Relation(master, foundation, edge)
  }

  def relationMasterMaster(masterFrom: VertexCore = generateMasterTree(), masterTo: VertexCore = generateMasterTree(), timestamp: String = giveMeARandomTimestamp): Relation = {
    val edgeProps = Map("timestamp" -> timestamp)
    val edge = EdgeCore(generateElementPropertiesFromMap(edgeProps), EDGE_MASTER_TREE_MASTER_TREE)
    Relation(masterFrom, masterTo, edge)
  }

  def relationBlockchainMaster(blockchain: VertexCore = generateBlockchain(), master: VertexCore = generateMasterTree(), timestamp: String = giveMeARandomTimestamp): Relation = {
    val edgeProps = Map("timestamp" -> timestamp)
    val edge = EdgeCore(generateElementPropertiesFromMap(edgeProps), EDGE_PUBLIC_CHAIN_MASTER_TREE)
    Relation(blockchain, master, edge)
  }

  /**
    * A blockchain has the label "PUBLIC_CHAIN"
    * and has for elements: timestamp, public_chain, transaction_id
    */
  def generateBlockchain(public_chain: String = giveMeRandomString, transaction_id: String = giveMeRandomString, timestamp: String = giveMeARandomTimestamp, hash: String = giveMeRandomString) = {
    val map = Map(PUBLIC_CHAIN -> public_chain, TIMESTAMP -> timestamp, TRANSACTION_ID -> transaction_id, HASH -> hash)
    VertexCore(generateElementPropertiesFromMap(map), LABEL_BLOCKCHAIN)
  }

  /**
    * A slave tree has the label "SLAVE_TREE"
    * and has for elements: hash, timestamp
    */
  def generateSlaveTree(hash: String = giveMeRandomString, timestamp: String = giveMeARandomTimestamp) = {
    val map = Map(HASH -> hash, TIMESTAMP -> timestamp)
    VertexCore(generateElementPropertiesFromMap(map), LABEL_FOUNDATION_TREE)
  }

  /**
    * A master tree has the label "MASTER_TREE"
    * and has for elements: hash, timestamp
    */
  def generateMasterTree(hash: String = giveMeRandomString, timestamp: String = giveMeARandomTimestamp) = {
    val map = Map(HASH -> hash, TIMESTAMP -> timestamp)
    VertexCore(generateElementPropertiesFromMap(map), LABEL_MASTER_TREE)
  }

  /**
    * A device has the label "DEVICE"
    * and has for elements: device_id, timestamp
    */
  def generateDevice(device_id: String = giveMeRandomString, timestamp: String = giveMeARandomTimestamp) = {
    val map = Map(DEVICE_ID -> device_id, TIMESTAMP -> timestamp)
    VertexCore(generateElementPropertiesFromMap(map), LABEL_DEVICE)

  }

  /**
    * A UPP has the label "UPP"
    * and has for elements: hash, signature, timestamp
    */
  def generateUpp(hash: String = giveMeRandomString, signature: String = giveMeRandomString, timestamp: String = giveMeARandomTimestamp) = {
    val map = Map(HASH -> hash, SIGNATURE -> signature, TIMESTAMP -> timestamp)
    VertexCore(generateElementPropertiesFromMap(map), LABEL_UPP)
  }

  def generateVertex(label: String, values: List[ElementProperty]) = {
    VertexCore(values, label)
  }

  def generateElementPropertiesFromMap(mapToConvert: Map[String, String]) = {
    mapToConvert.map(v => generateElementProperty(v._1, v._2)).toList
  }

  def generateElementProperty(key: String, value: String = giveMeRandomString) = {
    Util.convertProp(key, value)
  }

  def giveMeARandomTimestamp: String = {
    scala.util.Random.nextInt(Int.MaxValue - 1).toString
  }

  def giveMeRandomString: String = Random.alphanumeric.take(64).mkString


  def FakeInjectorNoRedis(port: Int = 8183): InjectorHelper = new InjectorHelper(List(new Binder {
    override def Config: ScopedBindingBuilder = bind(classOf[Config]).toProvider(customTestConfigProvider(port))

    override def Lock: ScopedBindingBuilder = bind(classOf[Lock]).to(classOf[FakeLock])
  })) {}

  def customTestConfigProvider(port: Int): ConfigProvider = new ConfigProvider {
    override def conf: Config = super.conf.withValue(
      "core.connector.port",
      ConfigValueFactory.fromAnyRef(port)
    )
  }

}

trait ExampleValues {
  val LABEL_DEVICE = "DEVICE"
  val LABEL_UPP = "UPP"
  val LABEL_MASTER_TREE = "MASTER_TREE"
  val LABEL_FOUNDATION_TREE = "SLAVE_TREE"
  val LABEL_BLOCKCHAIN = "PUBLIC_CHAIN"
  val EDGE_UPP_DEVICE = "UPP->DEVICE"
  val EDGE_ST_UPP = "SLAVE_TREE->UPP"
  val EDGE_SLAVE_TREE_SLAVE_TREE = "SLAVE_TREE->SLAVE_TREE"
  val EDGE_MASTER_TREE_SLAVE_TREE = "MASTER_TREE->SLAVE_TREE"
  val EDGE_MASTER_TREE_MASTER_TREE = "MASTER_TREE->MASTER_TREE"
  val EDGE_PUBLIC_CHAIN_MASTER_TREE = "PUBLIC_CHAIN->MASTER_TREE"
  val EDGE_CHAIN = "CHAIN"
  val HASH = "hash"
  val SIGNATURE = "signature"
  val TIMESTAMP = "timestamp"
  val DEVICE_ID = "device_id"
  val PUBLIC_CHAIN = "public_chain"
  val TRANSACTION_ID = "transaction_id"
}



@Singleton
class FakeLock @Inject() (lifecycle: Lifecycle, config: Config)(implicit scheduler: Scheduler)
  extends Lock {

  lazy val fakeLock = new RFakeLockDefault
  /**
    * Checks if the hash/payload already is stored in the cache.
    *
    * @param hash key
    * @return lock of the hash
    */
  override def createLock(hash: String): Option[RLock] = Some(fakeLock)

  /**
    * @return True if the lock is successfully connected to redis. False otherwise.
    */
  override def isConnected: Boolean = true
}

trait RFakeLock extends RLock {
  override def lock(leaseTime: Long, unit: TimeUnit): Unit = Nil
}

class RFakeLockDefault extends RFakeLock {
  override def getName: String = ???

  override def lockInterruptibly(leaseTime: Long, unit: TimeUnit): Unit = ???

  override def tryLock(waitTime: Long, leaseTime: Long, unit: TimeUnit): Boolean = ???

  override def forceUnlock(): Boolean = ???

  override def isLocked: Boolean = ???

  override def isHeldByThread(threadId: Long): Boolean = ???

  override def isHeldByCurrentThread: Boolean = ???

  override def getHoldCount: Int = ???

  override def lock(): Unit = ???

  override def lockInterruptibly(): Unit = ???

  override def tryLock(): Boolean = ???

  override def tryLock(time: Long, unit: TimeUnit): Boolean = ???

  override def unlock(): Unit = ???

  override def newCondition(): Condition = ???

  override def forceUnlockAsync(): RFuture[lang.Boolean] = ???

  override def unlockAsync(): RFuture[Void] = null

  override def unlockAsync(threadId: Long): RFuture[Void] = ???

  override def tryLockAsync(): RFuture[lang.Boolean] = ???

  override def lockAsync(): RFuture[Void] = ???

  override def lockAsync(threadId: Long): RFuture[Void] = ???

  override def lockAsync(leaseTime: Long, unit: TimeUnit): RFuture[Void] = ???

  override def lockAsync(leaseTime: Long, unit: TimeUnit, threadId: Long): RFuture[Void] = ???

  override def tryLockAsync(threadId: Long): RFuture[lang.Boolean] = ???

  override def tryLockAsync(waitTime: Long, unit: TimeUnit): RFuture[lang.Boolean] = ???

  override def tryLockAsync(waitTime: Long, leaseTime: Long, unit: TimeUnit): RFuture[lang.Boolean] = ???

  override def tryLockAsync(waitTime: Long, leaseTime: Long, unit: TimeUnit, threadId: Long): RFuture[lang.Boolean] = ???

  override def getHoldCountAsync: RFuture[Integer] = ???
}
