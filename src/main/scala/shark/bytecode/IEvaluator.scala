package shark.bytecode;

trait IEvaluator {

	def evaluate(row: AnyRef): Array[AnyRef]
}
