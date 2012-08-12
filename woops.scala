
object woops extends App {
  case class Argument(tag: String, expression: String)
  case class Execution(process: (String, String), output: Option[String], arg: Seq[Argument])

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

    val fileRef: Parser[String] = """(?:.\/)[\p{Alpha}\p{Digit}.\/_]+""".r
    val webRef: Parser[String] = """@https?://[\p{Alpha}\p{Digit}.\/_?&=]+""".r
    val expression = fileRef | webRef
  }

  val server = new ProcessingServer("http://localhost:8080/geoserver/wps")

  def validate(ex: Execution): Option[String] = {
    lazy val process: Either[String, ProcessName] = 
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
      in.arity.minOccurs.forall(_ == BigInt(0)) || ex.arg.exists(_.tag == in.name)

    def validateInput(d: ProcessDescriptor) = {
      d.inputs.find(i => !satisfied(i)) match {
        case Some(i) => 
          Left("Argument '%s' is required but not provided" format i.name)
        case None =>
          Right(())
      }
    }

    def validateArgs(d: ProcessDescriptor) = {
      ex.arg.find(a => d.inputs.forall(_.name != a.tag)) match {
        case None => Right(())
        case Some(a) => Left("Argument '%s' is not used by the process." format a.tag)
      }
    }

    val res =
      for { 
        p <- process.right
        desc <- describe(p).right
        _ <- validateArgs(desc).right
        _ <- validateInput(desc).right
      } yield ()

    res.left.toOption
  }

  Parser.parseAll(Parser.process, args.head) match {
    case Parser.Success(p, _) => println(validate(p))
    case ns: Parser.NoSuccess => println("Failed: \n" + ns)
  }
}
