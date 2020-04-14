package loadtest

import java.io.ByteArrayOutputStream
import java.nio.charset.StandardCharsets._

import com.google.cloud.ServiceOptions
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminClient
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminSettings
import com.google.cloud.bigtable.admin.v2.models.CreateTableRequest
import com.google.cloud.bigtable.admin.v2.models.GCRules
import com.google.cloud.bigtable.data.v2.BigtableDataClient
import com.google.cloud.bigtable.data.v2.BigtableDataSettings
import com.google.cloud.bigtable.data.v2.models.BulkMutation
import com.google.cloud.bigtable.data.v2.models.Mutation
import com.google.cloud.bigtable.data.v2.models.RowMutation
import com.google.protobuf.ByteString

import scala.util.Random

trait Base {
  val projectId = sys.env.getOrElse("BIGTABLE_PROJECT_ID", ServiceOptions.getDefaultProjectId())
  val instanceId = sys.env.getOrElse("BIGTABLE_INSTANCE_ID", "fake")
  val appProfileId = sys.env.get("CLOUD_BIGTABLE_APP_PROFILE_ID")
  val tableId = sys.env.getOrElse("CLOUD_BIGTABLE_TABLE_ID", "test")
  val familyId = "foo"
  val qualifier = "hello"

  lazy val admin = {
    val builder = BigtableTableAdminSettings
      .newBuilder()
      .setProjectId(projectId)
      .setInstanceId(instanceId)
    BigtableTableAdminClient.create(builder.build())
  }
  lazy val data = {
    val builder = BigtableDataSettings
      .newBuilder()
      .setProjectId(projectId)
      .setInstanceId(instanceId)

    appProfileId
      .foreach(builder.setAppProfileId)

    BigtableDataClient.create(builder.build())
  }
}

object CreateTable extends App with Base {
  val request = CreateTableRequest
    .of(tableId)
    .addFamily(familyId, GCRules.GCRULES.maxVersions(1))
  if (!admin.exists(tableId)) admin.createTable(request)
  println("done")
}
object CreateData extends App with Base {
  val random = new Random()
  val strings =
    (1 to 10000).map(_ => new String(random.alphanumeric.take(14).toArray))

  val content =
    Iterator.continually("hello").take(100 * 1024 / 2).mkString.getBytes(UTF_8)

  strings.grouped(200).foreach { strings =>
    val bulk = BulkMutation.create(tableId)
    strings.foreach { s =>
      val q = ByteString.copyFromUtf8(qualifier)
      val v = ByteString.copyFrom(content)
      val m = Mutation.create().setCell(familyId, q, v)
      bulk.add(s, m)
    }
    data.bulkMutateRows(bulk)
  }
  println("done")
}
object DeleteTable extends App with Base {
  if (admin.exists(tableId)) admin.deleteTable(tableId)
  println("done")
}
