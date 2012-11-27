package shark.bytecode

import scala.reflect.Manifest._

import org.apache.hadoop.hive.ql.exec.ExprNodeEvaluator

class Evaluator(evals: Array[ExprNodeEvaluator]) extends IEvaluator {

  val loader = new BytecodeLoader
  
  val delegate: IEvaluatorDelegate = {
    val tuple = generateBytecode
    val klass = loader.loadBytecode(tuple._1, tuple._2)
    klass.newInstance.asInstanceOf[IEvaluatorDelegate]
  }
  
  
  def evaluate(row: AnyRef): Array[AnyRef] = {
    if(evals.isEmpty) Array[AnyRef]() else {
      delegate.evaluate(evals, row)
    }
  }
  
  private def generateBytecode(): Tuple2[String, Array[Byte]] = {
    val className = "shark.bytecode.Evaluator1"
    val newClass = new Bytecode(className, Array[String](classOf[IEvaluatorDelegate].getName))
    val exprNodeEvaluatorClassName = classOf[ExprNodeEvaluator].getName
    val parameters = Array[Manifest[_]](Bytecode.arrayManifest[ExprNodeEvaluator], Object)
    val ret = Bytecode.arrayManifest[AnyRef]
    
    def seq(index: Int): Seq[MethodInsn] = List(
      ALOAD(3),
      LDC(index),
      ALOAD(1),
      LDC(index),
      AALOAD,
      ALOAD(2),
      DISPATCH(INVOKEVIRTUAL, exprNodeEvaluatorClassName,"evaluate", Array(Object), Object), 
      AASTORE   
    )
    
    val numberOfParams = evals.length
    val newSeq = Range(0,numberOfParams).foldLeft(List[MethodInsn]())((s, index) => s ++ seq(index))
    
    val methodInsn =  List(
      ALOAD(1), 
      ARRAYLENGTH, 
      ANEWARRAY(Object),
      ASTORE(3)
    )++
    newSeq ++
    List(
      ALOAD(3),
      ARETURN,
      MAX(4,4)
    )
    newClass.addMethod("evaluate", parameters, ret)(methodInsn :_*)
    val bytes = newClass.collect
    (className, bytes)
  }
}