package shark.bytecode

import scala.reflect.Manifest._
import org.objectweb.asm._
import org.objectweb.asm.Opcodes._
import Predef.{any2stringadd => _, _}
import org.objectweb.asm.util.CheckClassAdapter
import java.io.PrintWriter
import java.io.StringWriter

abstract sealed class Version {
  implicit def versionToInt(v: Version): Int = v.opcode

  def opcode: Int
}
object V1_6 extends Version {
  override def opcode: Int = Opcodes.V1_6
}


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

  def addField(name: String, klass: Class[_]) = {
    
  }

  def addMethod(
    name: String,
    parameters: Array[_ <: Manifest[_]] = Array[Manifest[_]](),
    returnType: Manifest[_] = Unit)(insns: MethodInsn*) = {
    val signature = Bytecode.methodDescriptor(parameters, returnType)
    val mv: MethodVisitor = cv.visitMethod(ACC_PUBLIC, name, signature, null, null)
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
    if (defaultConstructor)
      addDefaultConstructor()
    cv.visitEnd()
    cw.toByteArray()
  }
  
}

abstract sealed class MethodInsn {
  def apply(mv: MethodVisitor)
}
object RETURN extends MethodInsn {
  override def apply(mv: MethodVisitor) = mv.visitInsn(Opcodes.RETURN)
}
object IRETURN extends MethodInsn {
  override def apply(mv: MethodVisitor) = mv.visitInsn(Opcodes.IRETURN)
}
object ARETURN extends MethodInsn {
  override def apply(mv: MethodVisitor) = mv.visitInsn(Opcodes.ARETURN)
}
case class ILOAD(x: Int) extends MethodInsn {
  override def apply(mv: MethodVisitor) = mv.visitVarInsn(Opcodes.ILOAD, x)
}
case class ALOAD(x: Int) extends MethodInsn {
  override def apply(mv: MethodVisitor) = mv.visitVarInsn(Opcodes.ALOAD, x)
}
case class LDC(x: Any) extends MethodInsn {
  override def apply(mv: MethodVisitor) = mv.visitLdcInsn(x)
}
object ACONST_NULL extends MethodInsn {
  override def apply(mv: MethodVisitor) = mv.visitInsn(Opcodes.ACONST_NULL)
}
object ICONST_0 extends MethodInsn {
  override def apply(mv: MethodVisitor) = mv.visitInsn(Opcodes.ICONST_0)
}
case class ISTORE(x: Int) extends MethodInsn {
  override def apply(mv: MethodVisitor) = mv.visitVarInsn(Opcodes.ISTORE, x)
}
case class ASTORE(x: Int) extends MethodInsn {
  override def apply(mv: MethodVisitor) = mv.visitVarInsn(Opcodes.ASTORE, x)
}
object AALOAD extends MethodInsn {
  override def apply(mv: MethodVisitor) = mv.visitInsn(Opcodes.AALOAD)
}
object AASTORE extends MethodInsn {
  override def apply(mv: MethodVisitor) = mv.visitInsn(Opcodes.AASTORE)
}
object IADD extends MethodInsn {
  override def apply(mv: MethodVisitor) = mv.visitInsn(Opcodes.IADD)
}
object ISUB extends MethodInsn {
  override def apply(mv: MethodVisitor) = mv.visitInsn(Opcodes.ISUB)
}
object IMUL extends MethodInsn {
  override def apply(mv: MethodVisitor) = mv.visitInsn(Opcodes.IMUL)
}
object IDIV extends MethodInsn {
  override def apply(mv: MethodVisitor) = mv.visitInsn(Opcodes.IDIV)
}
object ARRAYLENGTH extends MethodInsn {
  override def apply(mv: MethodVisitor) = mv.visitInsn(Opcodes.ARRAYLENGTH)
}
case class ANEWARRAY(m: Manifest[_]) extends MethodInsn {
  override def apply(mv: MethodVisitor) = mv.visitTypeInsn(Opcodes.ANEWARRAY, "java/lang/Object")
}
object DUP extends MethodInsn {
  override def apply(mv: MethodVisitor) = mv.visitInsn(Opcodes.DUP)
}
case class DISPATCH(dispatchType: DispatchType = INVOKEVIRTUAL, owner: String,
  name: String, parameters: Array[Manifest[_]] = Array(),
  returnType: Manifest[_] = Unit) extends MethodInsn {
  implicit def dispatchTypeToInt(d: DispatchType) = d.opcode

  override def apply(mv: MethodVisitor) = {
    mv.visitMethodInsn(dispatchType, Bytecode.classNameInVM(owner), name, Bytecode.methodDescriptor(parameters, returnType))
  }
}

case class MAX(stack: Int, locals: Int) extends MethodInsn {
  override def apply(mv: MethodVisitor) = mv.visitMaxs(stack, locals)
}

abstract sealed class AccessFlag {
  implicit def accessFlagToInt(a: AccessFlag):Int = a.opcode

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

object Bytecode {

  def classNameInVM(className: String) = className.replace(".", "/")
  
  def classDescriptor(k: Manifest[_]) = k match {
    case Unit => "V"
    case Int => "I"
    case _ => {
      val klass = k.erasure
      if(klass.isArray()) {
        "[L" + classNameInVM(klass.getComponentType().getName()) + ";"
      }else "L" + classNameInVM(k.erasure.getName()) + ";"
    }
  }
  
  def methodDescriptor(parameters: Array[_ <: Manifest[_]], returnType: Manifest[_]) = {
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
