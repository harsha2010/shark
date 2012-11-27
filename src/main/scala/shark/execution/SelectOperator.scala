package shark.execution

import org.apache.hadoop.hive.ql.exec.{ExprNodeEvaluator, ExprNodeEvaluatorFactory}
import org.apache.hadoop.hive.ql.exec.{SelectOperator => HiveSelectOperator}
import org.apache.hadoop.hive.ql.plan.SelectDesc
import scala.collection.JavaConversions._
import scala.reflect.BeanProperty
import shark.bytecode.Evaluator

/**
 * An operator that does projection, i.e. selecting certain columns and
 * filtering out others.
 */
class SelectOperator extends UnaryOperator[HiveSelectOperator] {

  @BeanProperty var conf: SelectDesc = _

  @transient var evaluator: Evaluator = _

  override def initializeOnMaster() {
    conf = hiveOp.getConf()
  }

  override def initializeOnSlave() {
    if (!conf.isSelStarNoCompute) {
      val evals = conf.getColList().map(ExprNodeEvaluatorFactory.get(_)).toArray
      evals.foreach(_.initialize(objectInspector))
      evaluator = new Evaluator(evals)
    }
  }

  override def processPartition(split: Int, iter: Iterator[_]) = {
    if (conf.isSelStarNoCompute) {
      iter
    } else {
      iter.map { row => evaluator.evaluate(row.asInstanceOf[AnyRef])}
    }
  }
}
