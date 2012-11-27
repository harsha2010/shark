/**
 * Copyright (c) 2012 Yahoo! Inc. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License. See accompanying LICENSE file.
 */

package shark.bytecode

/**
 * A class loader that is responsible for loading bytecode at runtime.
 * @author harshars
 */
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
