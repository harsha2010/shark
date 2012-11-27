package shark.bytecode

import javax.crypto.Cipher.r
import java.io.File
import org.apache.commons.io.FileUtils
import org.apache.hadoop.hive.ql.exec.ExprNodeEvaluator
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector
import org.apache.hadoop.hive.ql.exec.ExprNodeEvaluatorFactory
import org.apache.hadoop.io.Text
import org.apache.hadoop.hive.serde2.objectinspector.InspectableObject
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils.ObjectInspectorCopyOption
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils
import org.junit.Assert.fail
import org.junit.Assert._
import org.junit.BeforeClass
import org.junit.Before
import org.junit.Test
import scala.Predef._
import scala.collection.JavaConversions._
import scala.reflect.Manifest._
import scala.sys.SystemProperties

class BytecodeTest {

  var col1: List[Text] = _
  var col1Type: TypeInfo = _
  var cola: List[Text] = _
  var colaType: TypeInfo = _
  var r: InspectableObject = _
 
  @Before
  def before() {
    col1 = List[Text](new Text("0"), new Text("1"), new Text("2"), new Text("3"));
    col1Type = TypeInfoFactory.getListTypeInfo(TypeInfoFactory.stringTypeInfo);
    cola = List[Text](new Text("a"), new Text("b"), new Text("c"));
    colaType = TypeInfoFactory.getListTypeInfo(TypeInfoFactory.stringTypeInfo);
    val data:Array[AnyRef] = Array[AnyRef](col1, cola);

    val names = List[String]("col1", "cola")

    val typeInfos = List[TypeInfo](col1Type, colaType)
    val dataType = TypeInfoFactory.getStructTypeInfo(names, typeInfos);

    r = new InspectableObject
    r.o = data;
    r.oi = TypeInfoUtils
      .getStandardWritableObjectInspectorFromTypeInfo(dataType);
  }
  @Test
  def testGenerateMethod1(): Unit = {
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
  def testGenerateMethod2(): Unit = {
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
      val doSomethingMthd = klass.getDeclaredMethod("doSomething", classOf[Int], classOf[Int])
      val arg0 = 10.asInstanceOf[Object]
      val arg1 = 20.asInstanceOf[Object]
      val result = doSomethingMthd.invoke(klass.newInstance(), arg0, arg1)
      assertEquals(result,10)
    }catch {
      case e: Exception => {
        e.printStackTrace()
        fail("No Exceptions should be encountered when executing a correctly defined method")
      }
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
    val addMthd = klass.getDeclaredMethod("add", classOf[Int], classOf[Int])
    val arg0 = 10.asInstanceOf[Object]
    val arg1 = 20.asInstanceOf[Object]
    val result = addMthd.invoke(klass.newInstance(), arg0, arg1)
    assertEquals(result, 30)
  }
    
  @Test
  def testGenerateMethod4(): Unit = {
    // Object evaluate (Evaluator eval, Object row)
    val className = "shark.Foo4"
    val newClass = new Bytecode(className)
    val exprNodeEvaluatorClassName = classOf[ExprNodeEvaluator].getName
    val parameters = Array[Manifest[_]](Bytecode.classManifest[ExprNodeEvaluator], Object)
    val ret = Object
    newClass.addMethod("evaluate", parameters, ret)(
      ALOAD(1),
      ALOAD(2),
      DISPATCH(INVOKEVIRTUAL, exprNodeEvaluatorClassName,"evaluate", Array(Object), Object),
      ARETURN,
      MAX(2, 3))
    val bytes = newClass.collect
    val exprDesc = new ExprNodeColumnDesc(colaType, "cola", "", false)
    val eval = ExprNodeEvaluatorFactory.get(exprDesc);
    val newClassLoader = new BytecodeLoader(bytes)
    val klass = newClassLoader.loadClass(className)
    val instance = klass.newInstance
    // evaluate on row
    val resultOI = eval.initialize(r.oi);
    val evaluateMthd = klass.getDeclaredMethod("evaluate", classOf[ExprNodeEvaluator], classOf[AnyRef])
    //val resultO = eval.evaluate(r.o).asInstanceOf[List[_]]
    val resultO = evaluateMthd.invoke(instance, eval, r.o).asInstanceOf[List[_]]
    assertEquals(resultO.size, 2)
  }

  class BytecodeLoader(bytes: Array[Byte]) extends ClassLoader {
    override def findClass(name: String): Class[_] = {
      defineClass(name, bytes, 0, bytes.length)
    }
  }

}