package shark.bytecode

import org.junit.Test
import org.junit.Assert.fail
import org.junit.Assert._
import org.apache.commons.io.FileUtils
import java.io.File
import scala.sys.SystemProperties
import scala.Predef._
import scala.reflect.Manifest._

class BytecodeTest {

  @Test
  def testGenerateMethod1() = {
    val className = "shark.Foo1"
    val newClass = new Bytecode(className)
    newClass.addMethod("doSomething", Array(Int,Any))(
    		RETURN,
    		MAX(1,3)
    )
    val bytes = newClass.collect
   
    val results = Bytecode.verify(bytes)
    assertTrue(results, results.isEmpty())
  }
  
   @Test
  def testGenerateMethod2() = {
    val className = "shark.Foo2"
    val newClass = new Bytecode(className)
    newClass.addMethod("doSomething", Array(Int, Int), Int)(
    		ILOAD(1),
    		IRETURN,
    		MAX(1,3)
    )
    val bytes = newClass.collect
   
    val newClassLoader = new BytecodeLoader(bytes)
    val klass = newClassLoader.loadClass(className)
    try{
    	val result = klass.getDeclaredMethod("doSomething", classOf[Int], classOf[Int]).invoke(klass.newInstance(), 10.asInstanceOf[Object],20.asInstanceOf[Object])
    	assertEquals(result,10)
    }catch {
    	case e:Exception => e.printStackTrace();fail("No Exceptions should be encountered when executing a correctly defined method")
    }
  }
   
    @Test
  def testGenerateMethod3() = {
    val className = "shark.Foo3"
    val newClass = new Bytecode(className)
    newClass.addMethod("add", Array(Int,Int),Int)(
    		ILOAD(1),
    		ILOAD(2),
    		IADD,
    		IRETURN,
    		MAX(2,3)
    )
    val bytes = newClass.collect
   
    val newClassLoader = new BytecodeLoader(bytes)
    val klass = newClassLoader.loadClass(className)
    val result = klass.getDeclaredMethod("add", classOf[Int], classOf[Int]).invoke(klass.newInstance(), 10.asInstanceOf[Object],20.asInstanceOf[Object])
    assertEquals(result, 30)
  }
    
  @Test
  def testGenerateClass() = {
    val className = "com.yahoo.jasmin.Foo"
    val newClass = new Bytecode(className)
    val bytes = newClass.collect
  
    val newClassLoader = new BytecodeLoader(bytes)
    try {
      val klass = newClassLoader.loadClass(className)
      klass.newInstance
    }catch {
      case e:Exception => e.printStackTrace();fail("No Exceptions should be encountered when loading a correctly defined class")
    }
    assertTrue(true)
  }

  class BytecodeLoader(bytes: Array[Byte]) extends ClassLoader {
    override def findClass(name: String): Class[_] = {
      defineClass(name, bytes, 0, bytes.length)
    }
  }
  
}