package object woops {
  type ProcessInput = net.opengis.wps10.InputDescriptionType

  private def conv(i: java.math.BigInteger): BigInt = BigInt(i.toByteArray)

  implicit def enrichInput(p: ProcessInput) = new {
    def name: String = p.getIdentifier.getValue
    def `type`: ProcessData =
      if (p.getLiteralData != null)
        (for { 
          l <- Option(p.getLiteralData)
          d <- Option(l.getDataType)
          v <- Option(d.getValue)
        } yield SimpleData(v)).getOrElse(UnknownData)
      else if (p.getComplexData != null) 
        // TODO: Parse the other supported formats
        ComplexData(p.getComplexData.getDefault.getFormat.getMimeType, Set.empty) 
      else
        UnknownData
        // sys.error("ProcessInput: '%s' specifies neither Literal nor Complex data" format p.getIdentifier)
    def arity: Arity = 
      Arity(Some(conv(p.getMinOccurs)), Some(conv(p.getMaxOccurs)))
  }
}

package woops {
  case class ProcessName(name: String, `abstract`: String)

  sealed trait ProcessData
  case class SimpleData(xsdType: String) extends ProcessData
  case class ComplexData(mimeType: String, alternates: Set[String]) extends ProcessData
  case object UnknownData extends ProcessData

  case class Arity(minOccurs: Option[BigInt], maxOccurs: Option[BigInt])

  // case class ProcessInput(name: String, `type`: ProcessData, arity: Arity)
  case class ProcessOutput(name: String, `type`: ProcessData)

  case class ProcessDescriptor(
    pname: ProcessName, inputs: Seq[ProcessInput], outputs: Seq[ProcessOutput])
}
