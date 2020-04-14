package loadtest

import java.io.ByteArrayOutputStream
import java.{util => ju}

import com.google.cloud.ServiceOptions
import com.google.cloud.bigtable.data.v2.BigtableDataClient
import com.google.cloud.bigtable.data.v2.BigtableDataSettings
import com.google.cloud.bigtable.data.v2.models.Query
import com.google.cloud.bigtable.data.v2.models.Row
import com.google.protobuf.ByteString
import io.gatling.commons.stats.KO
import io.gatling.commons.stats.OK
import io.gatling.commons.util.Clock
import io.gatling.commons.validation.Failure
import io.gatling.commons.validation.Success
import io.gatling.commons.validation.Validation
import io.gatling.core.CoreComponents
import io.gatling.core.Predef._
import io.gatling.core.action.Action
import io.gatling.core.action.RequestAction
import io.gatling.core.action.builder.ActionBuilder
import io.gatling.core.check.Check
import io.gatling.core.check.CheckResult
import io.gatling.core.config.GatlingConfiguration
import io.gatling.core.protocol.Protocol
import io.gatling.core.protocol.ProtocolComponents
import io.gatling.core.protocol.ProtocolKey
import io.gatling.core.protocol.Protocols
import io.gatling.core.session.Expression
import io.gatling.core.session.StaticStringExpression
import io.gatling.core.stats.StatsEngine
import io.gatling.core.structure.ScenarioContext

import scala.collection.JavaConverters._
import scala.concurrent.duration._
import scala.util.Random

class CloudBigtableSimulation extends Simulation {
  import CloudBigtableSimulation._

  val dataSettings = {
    val builder = BigtableDataSettings
      .newBuilder()
      .setProjectId(projectId)
      .setInstanceId(instanceId)

    appProfileId
      .foreach(builder.setAppProfileId)

    builder.build()
  }

  val dataClient = BigtableDataClient.create(dataSettings)

  val query = Query.create(tableId)
  val ids = dataClient
    .readRows(query)
    .iterator()
    .asScala
    .map(_.getKey().toStringUtf8())
    .toVector

  println("ids: " + ids.size)

  val random = new Random()

  def feeder(i: Int) =
    Iterator.continually(Map(randomKey -> ids(random.nextInt(ids.size))))

  val storageProtocol = CloudBigtableProtocol(dataClient)

  def scn(i: Int) =
    scenario(s"cloudbigtable-$i")
      .feed(feeder(i))
      .exec(CloudBigtableBuilder("GET"))

  setUp(
    (1 to 16)
      .map(i => scn(i).inject(constantConcurrentUsers(500).during(10.seconds)))
      .toList
  ).protocols(CloudBigtableProtocol(dataClient))

  after(dataClient.close())
}

object CloudBigtableSimulation {
  val projectId = sys.env.getOrElse("BIGTABLE_PROJECT_ID", ServiceOptions.getDefaultProjectId())
  val instanceId = sys.env.getOrElse("BIGTABLE_INSTANCE_ID", "fake")
  val appProfileId = sys.env.get("CLOUD_BIGTABLE_APP_PROFILE_ID")
  val tableId = sys.env.getOrElse("CLOUD_BIGTABLE_TABLE_ID", "test")
  val randomKey = "randomKey"

  case class CloudBigtableProtocol(servie: BigtableDataClient) extends Protocol
  object CloudBigtableProtocol {
    val protocolKey =
      new ProtocolKey[CloudBigtableProtocol, CloudBigtableComponents] {
        def protocolClass: Class[Protocol] =
          classOf[CloudBigtableProtocol].asInstanceOf[Class[Protocol]]
        def defaultProtocolValue(
            configuration: GatlingConfiguration
        ): CloudBigtableProtocol = ???
        def newComponents(
            coreComponents: CoreComponents
        ): CloudBigtableProtocol => CloudBigtableComponents =
          CloudBigtableComponents(coreComponents, _)
      }
  }

  case class CloudBigtableComponents(
      coreComponents: CoreComponents,
      protocol: CloudBigtableProtocol
  ) extends ProtocolComponents {
    def onStart: Session => Session = ProtocolComponents.NoopOnStart
    def onExit: Session => Unit = ProtocolComponents.NoopOnExit
  }

  case class CloudBigtableBuilder(tag: String) extends ActionBuilder {
    def build(ctx: ScenarioContext, next: Action): Action = {

      val components = ctx.protocolComponentsRegistry.components(
        CloudBigtableProtocol.protocolKey
      )
      GetAction(ctx, tag, next, components)
    }
  }

  case class GetAction(
      ctx: ScenarioContext,
      name: String,
      next: Action,
      components: CloudBigtableComponents
  ) extends RequestAction {

    def statsEngine: StatsEngine = ctx.coreComponents.statsEngine
    def clock: Clock = ctx.coreComponents.clock

    def requestName: Expression[String] =
      session => Success(session.attributes(randomKey).asInstanceOf[String])

    def sendRequest(requestName: String, session: Session): Validation[Unit] = {
      if (ctx.throttled)
        ctx.coreComponents.throttler
          .throttle(
            session.scenario,
            () => run(components.protocol.servie, session, requestName)
          )
      else
        run(components.protocol.servie, session, requestName)
    }

    private[this] val check: Check[Option[Row]] =
      new Check[Option[Row]] {
        def check(
            response: Option[Row],
            session: Session,
            preparedCache: ju.Map[Any, Any]
        ): Validation[CheckResult] = {
          response match {
            case v @ Some(value) if value.getCells.size() > 0 =>
              Success(CheckResult(v, None))
            case _ =>
              Failure("response is null")
          }
        }
      }

    private def run(
        service: BigtableDataClient,
        session: Session,
        requestName: String
    ): Unit = {
      val startTimestamp = clock.nowMillis
      val result = Option(service.readRow(tableId, requestName))
      val endTimestamp = clock.nowMillis

      val (checkSaveUpdated, checkError) = Check.check[Option[Row]](
        result,
        session,
        check :: Nil,
        null
      )

      val status = if (checkError.isEmpty) OK else KO
      val errorMessage = checkError.map(_.message)
      val withStatus =
        if (status == KO) checkSaveUpdated.markAsFailed else checkSaveUpdated
      statsEngine.logResponse(
        withStatus,
        requestName,
        startTimestamp = startTimestamp,
        endTimestamp = endTimestamp,
        status = status,
        responseCode = None,
        message = errorMessage
      )
      val newSession =
        withStatus.logGroupRequestTimings(
          startTimestamp = startTimestamp,
          endTimestamp = endTimestamp
        )
      if (status == KO) {
        logger.info(
          s"Request '$requestName' failed for user ${session.userId}: ${errorMessage.getOrElse("")}"
        )
      }
      next ! newSession
    }
  }
}
