package woops

import scala.collection.JavaConversions._
import org.geotools.data.wps._

case class Argument(tag: String, expression: Exp)
case class Execution(process: (String, String), output: Option[String], args: Seq[Argument])

sealed trait Exp
case class Literal(value: String) extends Exp
case class LocalRef(path: String) extends Exp
case class RemoteRef(url: String) extends Exp

object woops extends App {
  implicit def rightBias[L, R](e: Either[L, R]): Either.RightProjection[L,R] = e.right
  
  val factory = new net.opengis.wps10.impl.Wps10FactoryImpl

  object Parser extends scala.util.parsing.combinator.RegexParsers {
    val identPattern = """\p{Alpha}\p{Alnum}*"""
    val qualifiedIdentPattern = identPattern + ":" + identPattern
    val doubleQualifiedIdentPattern = Seq.fill(3)(identPattern).mkString(":")

    val ident: Parser[String] = identPattern.r
    val qualifiedIdent: Parser[(String, String)] = 
      for {
        m <- qualifiedIdentPattern.r
        Array(ns, local) = m.split(':')
      } yield (ns, local)

    val doubleQualifiedIdent: Parser[(String, String, String)] = 
      for {
        m <- doubleQualifiedIdentPattern.r
        Array(ns, local, res) = m.split(':')
      } yield (ns, local, res)

    val processArg: Parser[Argument] = 
      for { 
        id <- ident <~ "="
        expr <- expression
      } yield Argument(id, expr)

    val processArgs: Parser[Seq[Argument]] =  "(" ~> rep1sep(processArg, ",") <~ ")"

    val processWithDefaultResult: Parser[Execution] =
      for {
        id <- qualifiedIdent
        args <- processArgs
      } yield Execution(id, None, args)

    val processWithSpecifiedResult: Parser[Execution] =
      for {
        id <- doubleQualifiedIdent
        (ns, local, res) = id
        args <- processArgs
      } yield Execution((ns, local), Some(res), args)

    val process: Parser[Execution] =
      processWithDefaultResult | processWithSpecifiedResult

    val fileRef: Parser[LocalRef] = 
      for (path <- """(?:.\/)[\p{Alpha}\p{Digit}.\/_]+""".r) yield LocalRef(path)

    val webRef: Parser[RemoteRef] =
      for (url <- """@https?://[\p{Alpha}\p{Digit}.\/_?&=]+""".r) yield RemoteRef(url.tail)

    val numeric: Parser[Literal] =
      for (digits <- """[\p{Digit}.]+""".r) yield Literal(digits)

    val expression: Parser[Exp] = fileRef | webRef | numeric
  }

  val server = new ProcessingServer("http://localhost:8080/geoserver/wps")

  // TODO: This should be checking the expected data type for the parameter
  def asData(e: Exp): Any =
    e match {
      case Literal(value) => value
      case LocalRef(path) =>
        import org.geoscript.geometry.GML
        val input = factory.createInputType
        val data = factory.createDataType
        val complex = factory.createComplexDataType
        complex.getData.asInstanceOf[java.util.List[Any]]
          .add(GML.decode(new java.io.File(path)))
        data.setComplexData(complex)
        input.setData(data)
        input
      case RemoteRef(href) => 
        val input = factory.createInputReferenceType
        input.setHref(href)
        input
    }


  def sequence[L,R](es: Seq[Either[L,R]]): Either[L, Seq[R]] =
    (es foldRight (Right(Nil): Either[L,Seq[R]])) { (either, accum) =>
      for {
        r <- either
        acc <- accum
      } yield r +: acc
    }

  def validate(ex: Execution)
  : Either[String, org.geotools.data.wps.request.ExecuteProcessRequest]
  = {
    val process: Either[String, ProcessName] = 
      try {
        val name = ex.process._1 + ":" + ex.process._2
        server.getProcesses.find(_.name == name) match {
          case Some(x) => Right(x)
          case None => Left("No such process (" + ex.process + ")")
        }
      } catch {
        case ex => Left("Failure communicating with server")
      }

    def describe(p: ProcessName) = 
      try {
        Right(server.describe(p))
      } catch {
        case ex => Left("Failure communicating with server")
      }

    def satisfied(in: ProcessInput) = 
      in.arity.minOccurs.forall(_ == BigInt(0)) || ex.args.exists(_.tag == in.name)

    def validateInput(d: ProcessDescriptor) = {
      d.inputs.find(i => !satisfied(i)) match {
        case Some(i) => 
          Left("Argument '%s' is required but not provided" format i.name)
        case None =>
          Right(())
      }
    }

    def mkInput(d: ProcessDescriptor)(arg: Argument)
    : Either[String, (String, net.opengis.wps10.DataType)] =
      d.inputs.find(_.name == arg.tag) match {
        case None => Left("Argument '%s' is not used by this process." format arg.tag)
        case Some(inputSpec) => 
          Right(arg.tag, WPSUtils.createInputDataType(asData(arg.expression), inputSpec))
      }

    def mkInputs(d: ProcessDescriptor, args: Seq[Argument])
    : Either[String, Seq[(String, net.opengis.wps10.DataType)]] =
      sequence(args map mkInput(d))

    for {
      p <- process
      desc <- describe(p)
      inputs <- mkInputs(desc, ex.args)
    } yield {
      val request = server.specification.createExecuteProcessRequest(server.serviceUrl)
      request.setIdentifier(desc.pname.name)
      inputs.foreach { case (name, data) => request.addInput(name, Seq(data)) }
      request 
    }
  }

  Parser.parseAll(Parser.process, args.head) match {
    case Parser.Success(p, _) =>
      validate(p) match {
        case Left(error) => println("Failed with error: " + error)
        case Right(request) => 
          println("Succeeded, request is: " + request)
          // val response: org.geotools.data.wps.response.ExecuteProcessResponse =
          //   server.service.issueRequest(request)
          val bytes = new java.io.ByteArrayOutputStream
          request.performPostOutput(bytes)
          println(new String(bytes.toByteArray))
      }
    case ns: Parser.NoSuccess => println("Failed: \n" + ns)
  }
}
