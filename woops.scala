package woops

import scala.collection.JavaConversions._
import org.geotools.data.wps._

case class Argument(tag: String, expression: Exp)
case class Execution(process: (String, String), output: Option[String], args: Seq[Argument])

sealed trait Exp
case class Literal(value: String) extends Exp
case class LocalRef(path: String) extends Exp
case class RemoteRef(url: String) extends Exp
case class WFSRef(url: String, typename: String) extends Exp

object woops extends App {
  type InputData = Either[net.opengis.wps10.DataType, net.opengis.wps10.InputReferenceType]
  val asEObject = (_: InputData).fold(identity, identity)

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

    val wfsRef: Parser[WFSRef] =
      for {
        url <- """@wfs\+https?://[\p{Alpha}\p{Digit}:.\/_?&=]+""".r
        _ <- elem('#')
        typename <- """[\p{Alpha}\p{Digit}:.\/_?&=]+""".r
      } yield WFSRef(url drop ("@wfs+".size), typename)
        
    val numeric: Parser[Literal] =
      for (digits <- """[\p{Digit}.]+""".r) yield Literal(digits)

    val stringLiteral: Parser[Literal] =
      for {
        _ <- elem('"')
        text <- rep(elem("Unquoted Text", _ != '"'))
        _ <- elem('"')
      } yield Literal(text.mkString)

    val expression: Parser[Exp] = fileRef | webRef | wfsRef | numeric | stringLiteral
  }

  val server = new ProcessingServer("http://localhost:8080/geoserver/wps")

  // TODO: This should be checking the expected data type for the parameter
  def asData(e: Exp, input: ProcessInput): InputData =
    e match {
      case Literal(value) => 
        Left(WPSUtils.createInputDataType(value, input))
      case LocalRef(path) =>
        import org.geotools.xml.Parser, org.geotools.gml2.GMLConfiguration
        val parser = new Parser(new GMLConfiguration)
        val reader = new java.io.FileReader(new java.io.File(path))
        val geom = 
          try
            parser.parse(reader)
          finally
            reader.close()

        val param = WPSUtils.createInputDataType(
          geom,
          WPSUtils.INPUTTYPE_COMPLEXDATA,
          "application/wkt")
        println(param)
        Left(param)
      case RemoteRef(href) => 
        val ref = factory.createInputReferenceType
        ref.setHref(href)
        Right(ref)
      case WFSRef(href, typename) =>
        val encode = java.net.URLEncoder.encode(_: String, "UTF-8")
        val ref = factory.createInputReferenceType
        ref.setHref(href + "?service=WFS&version=1.0.0&format=gml&typename=" + encode(typename))
        Right(ref)
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
    : Either[String, (String, InputData)] =
      d.inputs.find(_.name == arg.tag) match {
        case None => Left("Argument '%s' is not used by this process." format arg.tag)
        case Some(inputSpec) => 
          Right((arg.tag, asData(arg.expression, inputSpec)))
      }

    def mkInputs(d: ProcessDescriptor, args: Seq[Argument])
    : Either[String, Seq[(String, InputData)]] =
      sequence(args map mkInput(d))

    for {
      p <- process
      desc <- describe(p)
      inputs <- mkInputs(desc, ex.args)
    } yield {
      val request = server.specification.createExecuteProcessRequest(server.serviceUrl)
      request.setIdentifier(desc.pname.name)
      inputs.foreach { case (name, data) => 
        request.addInput(name, Seq(asEObject(data)))
      }

      if (desc.outputs.size == 1) {
        val raw = server.service.createOutputDefinitionType(desc.outputs.head.name)
        desc.outputs.head.`type` match {
          case SimpleData(schema) => raw.setMimeType(schema)
          case ComplexData(schema, _) => raw.setMimeType(schema)
          case UnknownData => ()
        }
        request.setResponseForm(server.service.createResponseForm(null, raw))
      }

      // desc.outputs.foreach { request.addOutput } // println } 
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
