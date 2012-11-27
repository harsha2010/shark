package shark.bytecode;

import org.apache.hadoop.hive.ql.exec.ExprNodeEvaluator;

public interface IEvaluatorDelegate {

	public Object[] evaluate(ExprNodeEvaluator[] evals, Object row);
}
