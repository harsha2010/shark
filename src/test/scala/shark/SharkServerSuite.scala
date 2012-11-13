package shark

import java.io.{BufferedReader, InputStreamReader, PrintWriter}
import org.scalatest.{BeforeAndAfterAll, FunSuite, BeforeAndAfterEach, FlatSpec}
import java.sql.DriverManager
import org.scalatest.Assertions
import java.sql.Statement
import java.util.concurrent.CountDownLatch
import java.sql.Connection
import scala.actors.Actor
import java.util.concurrent.ConcurrentHashMap
import org.scalatest.matchers.ShouldMatchers
import org.scalatest.FlatSpec
import scala.collection.JavaConversions._
import scala.concurrent.ops._

class SharkServerSuite extends FunSuite with BeforeAndAfterAll  with BeforeAndAfterEach with ShouldMatchers {

  val WAREHOUSE_PATH = CliTestToolkit.getWarehousePath("server")

  val DRIVER_NAME  = "org.apache.hadoop.hive.jdbc.HiveDriver"
  val TABLE = "test"

  Class.forName(DRIVER_NAME)
  
  override def beforeAll() {
    
    spawn {
      SharkServer.main(Array[String]())
    }
    Thread.sleep(8000)
    createTable
    createCachedTable
  }

  override def afterAll() {
    dropTable
    dropCachedTable
    SharkServer.stop
  
  }

  
  test("Read from existing table") {
    val stmt = createStatement
    val rs = stmt.executeQuery("select key, count(*) from test group by key")
    rs.next ; 
    val count = rs.getInt(1)
    println("Count : " , count)
    count should equal (4)
  } 
  /*
  test("Read from existing cached table") {
    val stmt = createStatement
    val rs = stmt.executeQuery("select count(*) from test_cached where key = 406")
    rs.next ; 
    val count = rs.getInt(1)
    count should equal (4)
  } 
  
  test("Concurrent reads without state") {
    Class.forName(DRIVER_NAME)
    val n = 10
    val latch = new CountDownLatch(n)
    var results = scala.collection.mutable.ListBuffer[Int]()
    class ManagedReadActor(threshold:Int) extends Actor {
          def exec = {
            val con = getConnection
            val setThresholdStmt = con.createStatement()
            setThresholdStmt.executeQuery("set threshold = " + threshold)
            val stmt = con.createStatement()
            val rs = stmt.executeQuery("select  count(*) from test_cached where key = " +
              "${hiveconf:threshold}")
            
            rs.next
            val value = rs.getInt(1)
            results.synchronized(results.prepend(value))
          }
          
          def act = {
            try {
              exec
            }finally {
              latch.countDown
            }
          }
    }

    Range(0, n).foreach(_ => new ManagedReadActor(406).start)
    latch.await
    println("Foo: " , results)
    assert(!results.isEmpty)
    val pairwiseResults =  results zip results.tail
    val option = pairwiseResults.find(x => x._1 != x._2)
    assert(option.isEmpty)
  }
  
  test("Concurrent reads with state") {
    Class.forName(DRIVER_NAME)
    val n = 3
    val latch = new CountDownLatch(n)
    var results:collection.mutable.ConcurrentMap[Int, Int] = new ConcurrentHashMap[Int,Int]
    class ManagedReadActor(threshold:Int) extends Actor {
          def exec = {
            val con = getConnection
            val setThresholdStmt = con.createStatement()
            setThresholdStmt.executeQuery("set threshold = " + threshold)
            val stmt = con.createStatement()
            val rs = stmt.executeQuery("select  count(*) from test where key = " +
              "${hiveconf:threshold}")
            
            rs.next
            val value = rs.getInt(1)
            results.putIfAbsent(threshold, value)
          }
          
          def act = {
            try {
              exec
            }finally {
              latch.countDown
            }
          }
    }

    List(238, 406, 401).foreach(index => new ManagedReadActor(index).start)
    latch.await
    results should be (Map[Int, Int](238 ->2, 406 -> 4, 401 -> 5))
  }
  
  test("Concurrent reads of cached table with state") {
    Class.forName(DRIVER_NAME)
    val n = 3
    val latch = new CountDownLatch(n)
    var results:collection.mutable.ConcurrentMap[Int, Int] = new ConcurrentHashMap[Int,Int]
    class ManagedReadActor(threshold:Int) extends Actor {
          def exec = {
            val con = getConnection
            val setThresholdStmt = con.createStatement()
            setThresholdStmt.executeQuery("set threshold = " + threshold)
            val stmt = con.createStatement()
            val rs = stmt.executeQuery("select  count(*) from test_cached where key = " +
              "${hiveconf:threshold}")
            
            rs.next
            val value = rs.getInt(1)
            results.putIfAbsent(threshold, value)
          }
          
          def act = {
            try {
              exec
            }finally {
              latch.countDown
            }
          }
    }

    List(238, 406, 401).foreach(index => new ManagedReadActor(index).start)
    latch.await
    results should be (Map[Int, Int](238 ->2, 406 -> 4, 401 -> 5))
  }
  */
  def getConnection:Connection  = DriverManager.getConnection("jdbc:hive://localhost:10000/default", "", "")
  
  def createStatement:Statement = {
    getConnection.createStatement()
  }
  
  def createTable() = {
    val dataFilePath = System.getenv("HIVE_DEV_HOME") + "/data/files/kv1.txt"
    val stmt = createStatement
    stmt.executeQuery("DROP TABLE IF EXISTS test")
    stmt.executeQuery("CREATE TABLE test(key int, val string)")
    stmt.executeQuery("LOAD DATA LOCAL INPATH '" + dataFilePath+ "' OVERWRITE INTO TABLE test")
  }
  
  def createCachedTable() = {
    val stmt = createStatement
    stmt.executeQuery("DROP TABLE IF EXISTS test_cached")
    createStatement.executeQuery("CREATE TABLE test_cached as select * from test")
  }
  
  def dropTable(implicit table:String = TABLE) = {
    val stmt = createStatement
    val sql = "DROP TABLE " + table 
    val rs = stmt.executeQuery(sql)
  }
  
  def dropCachedTable = dropTable(TABLE + "_cached")
}