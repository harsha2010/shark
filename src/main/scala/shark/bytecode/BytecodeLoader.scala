package shark.bytecode

class BytecodeLoader(loader: ClassLoader) extends ClassLoader(loader) {

  private val cachedClasses = scala.collection.mutable.Map[String, Class[_]]()
  
  def this() {
    this(classOf[BytecodeLoader].getClassLoader())
  }
  
  override def loadClass(name: String, resolve: Boolean): Class[_] = {
    val klass = cachedClasses.getOrElse(name, super.loadClass(name, resolve))
    if(resolve) {
      resolveClass(klass)
    }
    klass
  }
  def loadBytecode(name: String, bytes: Array[Byte]): Class[_] = {
    val klass = defineClass(name, bytes, 0, bytes.length)
    cachedClasses.put(name, klass)
    klass
  }
  override def findClass(name: String): Class[_] = {
    cachedClasses.getOrElse(name, super.findClass(name))
  }
  
}