import org.geotools.data.wps._

object listprocesses /* extends App */ {
  import scala.collection.JavaConversions._

  val args = Seq("http://localhost:8080/geoserver/wps")
  
  val url = new java.net.URL(_: String)
  val wpsFromUrl = new WebProcessingService(_: java.net.URL)
  val processName = (_: net.opengis.wps10.ProcessBriefType).getIdentifier.getValue
  val processDesc = (_: net.opengis.wps10.ProcessBriefType).getAbstract.getValue

  val serviceUrl = url(args.head)
  val service = wpsFromUrl(serviceUrl)
  val specification = new WPS1_0_0

  val capsReq = specification.createGetCapabilitiesRequest(serviceUrl)
  val capsResponse = service.issueRequest(capsReq)
  val processOfferings =
    capsResponse.getCapabilities.getProcessOfferings.getProcess.asInstanceOf[java.util.List[net.opengis.wps10.impl.ProcessBriefTypeImpl]]

  processOfferings.foreach (p => println(processName(p) ++ ": " ++ processDesc(p)))
}

case class ProcessName(name: String, `abstract`: String)

sealed trait ProcessData
case class SimpleData(xsdType: String) extends ProcessData
case class ComplexData(mimeType: String, alternates: Set[String]) extends ProcessData

case class Arity(minOccurs: Option[BigInt], maxOccurs: Option[BigInt])

case class ProcessInput(name: String, `type`: ProcessData, arity: Arity)
case class ProcessOutput(name: String, `type`: ProcessData)

case class ProcessDescriptor(
  pname: ProcessName, inputs: Seq[ProcessInput], outputs: Seq[ProcessOutput])

class ProcessingServer(url: String) {
  import scala.collection.JavaConversions._

  val serviceUrl = new java.net.URL(url)
  val service = new WebProcessingService(serviceUrl)
  val specification = new WPS1_0_0

  def getProcesses(): Seq[ProcessName] = {
    val capsReq = specification.createGetCapabilitiesRequest(serviceUrl)
    val capsResponse = service.issueRequest(capsReq)
    val processOfferings =
      capsResponse.getCapabilities.getProcessOfferings.getProcess.asInstanceOf[java.util.List[net.opengis.wps10.impl.ProcessBriefTypeImpl]]
    processOfferings map (p =>
      ProcessName(p.getIdentifier.getValue, p.getAbstract.getValue))
  }

  def describe(p: ProcessName): ProcessDescriptor = {
    val descReq = specification.createDescribeProcessRequest(serviceUrl)
    descReq.setIdentifier(p.name)
    val descResponse = service.issueRequest(descReq)
      .getProcessDesc.getProcessDescription.asInstanceOf[java.util.List[net.opengis.wps10.ProcessDescriptionType]]

    val inputs = descResponse.head.getDataInputs.getInput.asInstanceOf[java.util.List[net.opengis.wps10.InputDescriptionType]]
    val outputs = descResponse.head.getProcessOutputs.getOutput.asInstanceOf[java.util.List[net.opengis.wps10.OutputDescriptionType]]

    val bi = (n: java.math.BigInteger) => scala.math.BigInt(n.toByteArray)

    val simpleIn = (d: net.opengis.wps10.LiteralInputType) =>
      SimpleData(d.getDataType.getValue)

    val simpleOut = (d: net.opengis.wps10.LiteralOutputType) =>
      SimpleData(d.getDataType.getValue)

    val complex = (d: net.opengis.wps10.SupportedComplexDataType) =>
      ComplexData(d.getDefault.getFormat.getMimeType, Set.empty) // TODO: Parse other supported types too.

    val inputFromOGC = (i: net.opengis.wps10.InputDescriptionType) => {
      val name = i.getIdentifier.getValue
      val `type` = 
        if (i.getLiteralData == null)
          complex(i.getComplexData)
        else
          simpleIn(i.getLiteralData)
      val arity = Arity(Some(bi(i.getMinOccurs)), Some(bi(i.getMaxOccurs)))
      ProcessInput(name, `type`, arity)
    }

    val outputFromOGC = (o: net.opengis.wps10.OutputDescriptionType) => {
      val name = o.getIdentifier.getValue
      val `type` =
        if (o.getLiteralOutput == null)
          complex(o.getComplexOutput)
        else
          simpleOut(o.getLiteralOutput)
      ProcessOutput(name, `type`)
    }

    ProcessDescriptor(p, (inputs map inputFromOGC), (outputs map outputFromOGC))
  }
}
