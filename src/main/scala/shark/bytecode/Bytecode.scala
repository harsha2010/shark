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

import java.io.PrintWriter
import java.io.StringWriter
import org.objectweb.asm._
import org.objectweb.asm.Opcodes._
import org.objectweb.asm.util.CheckClassAdapter
import Predef.{ any2stringadd => _, _ }
import scala.reflect.Manifest._

/**
 * Scala DSL for Bytecode generation, intended
 * as a fluent wrapper around ASM to allow building
 * composable bytecode instructions easily.
 * @author harshars
 */
class Bytecode(val className: String,
  val interfaces: Array[String] =  Array[String](),
  val superClassName: String = classOf[Object].getName,
  defaultConstructor: Boolean = true,
  version: Version = V1_6,
  modifiers:Array[AccessFlag] = Array(PUBLIC)) {

  val cw: ClassWriter = new ClassWriter(0)
  val cv: ClassVisitor = {
    val visitor = new ClassAdapter(cw) {}
    visitor.visit(V1_6, Bytecode.computeAccessCode(modifiers),
      Bytecode.classNameInVM(className),
      null,
      Bytecode.classNameInVM(superClassName),
      interfaces.map(i => Bytecode.classNameInVM(i)))
    visitor
  }

  def addField(name: String, klass: Class[_]): Unit = {
    Unit
  }

  def addMethod(
    name: String,
    parameters: Array[_ <: Manifest[_]] = Array[Manifest[_]](),
    returnType: Manifest[_] = Unit,
    flags: Array[AccessFlag] = Array(PUBLIC))(insns: MethodInsn*): Unit = {
    val signature = Bytecode.methodDescriptor(parameters, returnType)
    val access = flags.map(_.opcode).sum
    val mv: MethodVisitor = cv.visitMethod(access, name, signature, null, null)
    mv.visitCode
    insns.foreach(insn => insn(mv))
    mv.visitEnd
  }

  private def addDefaultConstructor() = {
    addMethod("<init>", Array[Manifest[_]](), Unit)(
      ALOAD(0),
      DISPATCH(INVOKESPECIAL, "java.lang.Object", "<init>"),
      RETURN,
      MAX(1, 1))
  }

  def collect(): Array[Byte] = {
    if (defaultConstructor) {
      addDefaultConstructor()
    }
    cv.visitEnd()
    cw.toByteArray()
  }
}

abstract sealed class Version {

  implicit def versionToInt(v: Version): Int = v.opcode

  def opcode: Int

}
object V1_6 extends Version {

  override def opcode: Int = Opcodes.V1_6

}

abstract sealed class MethodInsn {

  def apply(mv: MethodVisitor): Unit

}

object RETURN extends MethodInsn {

  override def apply(mv: MethodVisitor): Unit = mv.visitInsn(Opcodes.RETURN)

}

object IRETURN extends MethodInsn {

  override def apply(mv: MethodVisitor): Unit = mv.visitInsn(Opcodes.IRETURN)

}

object ARETURN extends MethodInsn {

  override def apply(mv: MethodVisitor): Unit = mv.visitInsn(Opcodes.ARETURN)

}

case class ILOAD(x: Int) extends MethodInsn {

  override def apply(mv: MethodVisitor): Unit = mv.visitVarInsn(Opcodes.ILOAD, x)

}

case class ALOAD(x: Int) extends MethodInsn {

  override def apply(mv: MethodVisitor): Unit = mv.visitVarInsn(Opcodes.ALOAD, x)

}

case class LDC(x: Any) extends MethodInsn {

  override def apply(mv: MethodVisitor): Unit = mv.visitLdcInsn(x)

}

object ACONST_NULL extends MethodInsn {

  override def apply(mv: MethodVisitor): Unit = mv.visitInsn(Opcodes.ACONST_NULL)

}

object ICONST_0 extends MethodInsn {

  override def apply(mv: MethodVisitor): Unit = mv.visitInsn(Opcodes.ICONST_0)

}

case class ISTORE(x: Int) extends MethodInsn {

  override def apply(mv: MethodVisitor): Unit = mv.visitVarInsn(Opcodes.ISTORE, x)

}

case class ASTORE(x: Int) extends MethodInsn {

  override def apply(mv: MethodVisitor): Unit = mv.visitVarInsn(Opcodes.ASTORE, x)

}

object AALOAD extends MethodInsn {

  override def apply(mv: MethodVisitor): Unit = mv.visitInsn(Opcodes.AALOAD)

}

object AASTORE extends MethodInsn {

  override def apply(mv: MethodVisitor): Unit = mv.visitInsn(Opcodes.AASTORE)

}

object IADD extends MethodInsn {

  override def apply(mv: MethodVisitor): Unit = mv.visitInsn(Opcodes.IADD)

}

object ISUB extends MethodInsn {

  override def apply(mv: MethodVisitor): Unit = mv.visitInsn(Opcodes.ISUB)

}

object IMUL extends MethodInsn {

  override def apply(mv: MethodVisitor): Unit = mv.visitInsn(Opcodes.IMUL)

}

object IDIV extends MethodInsn {

  override def apply(mv: MethodVisitor): Unit = mv.visitInsn(Opcodes.IDIV)

}

object ARRAYLENGTH extends MethodInsn {

  override def apply(mv: MethodVisitor): Unit = mv.visitInsn(Opcodes.ARRAYLENGTH)

}

case class ANEWARRAY(m: Manifest[_]) extends MethodInsn {

  override def apply(mv: MethodVisitor): Unit = {
    val className = Bytecode.classNameInVM(m.erasure.getName())
    mv.visitTypeInsn(Opcodes.ANEWARRAY, className)
  }

}

object DUP extends MethodInsn {

  override def apply(mv: MethodVisitor): Unit = mv.visitInsn(Opcodes.DUP)

}

case class CHECKCAST(m: Manifest[_]) extends MethodInsn {

  override def apply(mv: MethodVisitor): Unit = {
    val className = Bytecode.classNameInVM(m.erasure.getName())
    mv.visitTypeInsn(Opcodes.CHECKCAST, className)
  }

}

case class DISPATCH(dispatchType: DispatchType = INVOKEVIRTUAL, owner: String,
  name: String, parameters: Array[Manifest[_]] = Array(),
  returnType: Manifest[_] = Unit) extends MethodInsn {

  implicit def dispatchTypeToInt(d: DispatchType): Int = d.opcode

  override def apply(mv: MethodVisitor): Unit = {
    val className = Bytecode.classNameInVM(owner)
    val methodDesc = Bytecode.methodDescriptor(parameters, returnType)
    mv.visitMethodInsn(dispatchType, className, name, methodDesc)
  }
}

case class MAX(stack: Int, locals: Int) extends MethodInsn {

  override def apply(mv: MethodVisitor): Unit = mv.visitMaxs(stack, locals)

}

abstract sealed class AccessFlag {

  implicit def accessFlagToInt(a: AccessFlag): Int = a.opcode

  def opcode: Int

}

object PUBLIC extends AccessFlag {

  def opcode: Int = Opcodes.ACC_PUBLIC

}

object PRIVATE extends AccessFlag {

  def opcode: Int = Opcodes.ACC_PRIVATE

}

object PROTECTED extends AccessFlag {

  def opcode: Int = Opcodes.ACC_PROTECTED

}

object STATIC extends AccessFlag {

  def opcode: Int = Opcodes.ACC_STATIC

}

object FINAL extends AccessFlag {

  def opcode: Int = Opcodes.ACC_FINAL

}

abstract sealed class DispatchType {

  def opcode: Int

}

object INVOKESPECIAL extends DispatchType {

  def opcode: Int = Opcodes.INVOKESPECIAL

}

object INVOKEVIRTUAL extends DispatchType {
  def opcode: Int = Opcodes.INVOKEVIRTUAL
}

object INVOKESTATIC extends DispatchType {

  def opcode: Int = Opcodes.INVOKESTATIC

}

object INVOKEINTERFACE extends DispatchType {

  def opcode: Int = Opcodes.INVOKEINTERFACE

}

object Bytecode {

  def classNameInVM(className: String): String = className.replace(".", "/")

  def classDescriptor(k: Manifest[_]): String = k match {
    case Unit => "V"
    case Int => "I"
    case _ => {
      val klass = k.erasure
      if (klass.isArray()) {
        "[L" + classNameInVM(klass.getComponentType().getName()) + ";"
      }else {
        "L" + classNameInVM(k.erasure.getName()) + ";"
      }
    }
  }

  def methodDescriptor(parameters: Array[_ <: Manifest[_]], returnType: Manifest[_]): String = {
    "(" + parameters.foldLeft("")((r, c) => r + Bytecode.classDescriptor(c)) + ")" +
      Bytecode.classDescriptor(returnType)
  }

  def computeAccessCode(accessFlags:Array[AccessFlag]):Int = accessFlags.foldLeft(0)((r,c) => r + c.opcode)

  def computeReturnInsn(returnType: Manifest[_]): List[MethodInsn] = returnType match {
    case Unit => List(RETURN)
    case _ => List(ACONST_NULL, ARETURN)
  }

  def verify(bytes:Array[Byte]):String = {
    val output = new StringWriter()
    CheckClassAdapter.verify(new ClassReader(bytes),false, new PrintWriter(output))
    output.toString()
  }

  def arrayManifest[T](implicit m: Manifest[T]): Manifest[_]  = arrayType(m)

  def classManifest[T](implicit m: Manifest[T]): Manifest[_] = m
}
