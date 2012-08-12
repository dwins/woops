package woops

import org.geotools.data.wps._

object listprocesses extends App {
  import scala.collection.JavaConversions._

  // val args = Seq("http://localhost:8080/geoserver/wps")
  val url = new java.net.URL(_: String)
  val wpsFromUrl = new WebProcessingService(_: java.net.URL)
  val processName = (_: net.opengis.wps10.ProcessBriefType).getIdentifier.getValue
  val processDesc = (_: net.opengis.wps10.ProcessBriefType).getAbstract.getValue

  def getCaps(baseUrl: String) = {
    val serviceUrl = url(baseUrl)
    val service = wpsFromUrl(serviceUrl)
    val specification = new WPS1_0_0

    val capsReq = specification.createGetCapabilitiesRequest(serviceUrl)
    val capsResponse = service.issueRequest(capsReq)
    capsResponse.getCapabilities.getProcessOfferings.getProcess.asInstanceOf[java.util.List[net.opengis.wps10.impl.ProcessBriefTypeImpl]]
  }

  args match {
    case Array(url) =>
      val server = new ProcessingServer(url)
      for (ProcessName(name, desc) <- server.getProcesses) 
        println("%s: %s" format(name, desc))
    case Array(url, name) =>
      val server = new ProcessingServer(url)
      server.getProcesses.find(_.name == name) match {
        case None => println("No process named '%s' found." format(name))
        case Some(p) =>
          println("%s: %s" format(p.name, p.`abstract`))
          val desc = server.describe(p)
          for (input <- desc.inputs)
            println("  %s: %s; %s" format(input.name, input.`type`, input.arity))
      }
    case _ => println("""
    | Call with one argument to list processes for a service (specify url)
    | Call with two arguments to lists inputs for a specific process (specify url and process name)
    """.stripMargin)
  }
  
  // processOfferings.foreach (p => println(processName(p) ++ ": " ++ processDesc(p)))
}

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

    val inputFromOGC = identity[ProcessInput] _

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

  def buildRequest(
    desc: ProcessDescriptor,
    inputs: Seq[(String, Any)],
    outputs: Option[Seq[String]])
  : org.geotools.data.wps.request.ExecuteProcessRequest
  = {
    val request = specification.createExecuteProcessRequest(serviceUrl)
    request.setIdentifier(desc.pname.name)
    for ((name, value) <- inputs) {
      desc.inputs.find(_.name == name) match {
        case Some(inputSpecification) =>
          val inputWrapper = WPSUtils.createInputDataType(value, inputSpecification)
          request.addInput(name, Seq(inputWrapper))
        case None =>
          sys.error("Input '%s' is not accepted for process '%s'" format (name, desc.pname.name))
      }
    }
    request
  }
}
