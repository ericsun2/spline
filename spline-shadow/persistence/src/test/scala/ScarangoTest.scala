/*
 * Copyright 2017 ABSA Group Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/*
 * Copyright 2017 ABSA Group Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


import java.util.UUID

import ArangoModel._
import com.outr.arango._
import com.outr.arango.managed._
import org.apache.commons.lang.builder.ToStringBuilder.reflectionToString
import org.scalatest.mockito.MockitoSugar
import org.scalatest.{FunSpec, Matchers}
import za.co.absa.spline.{model => splinemodel}
import za.co.absa.spline.model.{DataLineage, MetaDataset, TypedMetaDataSource}
import za.co.absa.spline.model.op.{BatchWrite, Read, Write}

import scala.concurrent.Await
import scala.concurrent.duration.Duration

object ArangoModel {
  case class Progress(_id: Option[String], _key: Option[String], _rev: Option[String], timestamp: Long, readCount: Long) extends DocumentOption
  case class Execution(_id: Option[String], _key: Option[String], _rev: Option[String], appId: String, appName: String, sparkVer: String, timestamp: Long) extends DocumentOption
//  case class Schema(attributes: Seq[String])
  case class Operation(_id: Option[String], _key: Option[String], _rev: Option[String], name: String, expression: String, outputSchema: Seq[String]) extends DocumentOption
  case class DataSource(`type`: String, path: String,  _rev: Option[String] = None, _id: Option[String] = None) extends DocumentOption {
    val _key: Option[String] = Some(path)
  }
  case class ProgressOf(_id: Option[String], _key: Option[String], _rev: Option[String], _from: String, _to: String) extends Edge with DocumentOption
}

class ScarangoTest extends FunSpec with Matchers with MockitoSugar {

  describe("scarango") {
    it("funspec") {
      object Database extends Graph("lineages") {
        val progress: VertexCollection[Progress] = vertex[Progress]("progress")
        val execution: VertexCollection[Execution] = vertex[Execution]("execution")
        val operation: VertexCollection[Operation] = vertex[Operation]("operation")
        val dataSource: VertexCollection[DataSource] = vertex[DataSource]("dataSource")
        val progressOf: EdgeCollection[ProgressOf] = edge[ProgressOf]("progressOf", ("progress", "execution"))
      }
      val result = Await.result(Database.init(), Duration.Inf)
      println("Init result: " + result)
//      print("Graph creation result: " + Await.result(Database.fruit.create(), Duration.Inf))
//      println(Await.result(Database.fruit.insert(Fruit("Apple")), Duration.Inf))
      val execution: Execution = Await.result(Database.execution.insert(Execution(None, None, None, "appId1", "appName1", "2.2", System.currentTimeMillis)), Duration.Inf)
      val progress: Progress = Await.result(Database.progress.insert(Progress(None, None, None, System.currentTimeMillis, 10)), Duration.Inf)
      val progressOf = Await.result(Database.progressOf.insert(ProgressOf(None, None, None, progress._id.get, execution._id.get)), Duration.Inf)
      val query = aql"FOR f IN progress RETURN f"
//      println(Await.result(Database.fruit.cursor(query), Duration.Inf))
    }
  }

  // TODO store full attribute information
  private def findOutputSchema(dataLineage: DataLineage, operation: splinemodel.op.Operation) : Seq[String] = {
    val metaDataset: MetaDataset = dataLineage.datasets.find((dts: MetaDataset) => dts.id == operation.mainProps.output).get
    metaDataset.schema.attrs.map(attrId => {
      dataLineage.attributes.find(_.id == attrId).get.name
    })
  }

  def transform(dataLineage: DataLineage): (Execution, Set[DataSource], Set[Operation], Set[Operation]) = {
    val execution = Execution(None, Some(dataLineage.id.toString), None, dataLineage.appId, dataLineage.appName, dataLineage.sparkVer, dataLineage.timestamp)
    val operations = dataLineage.operations.map(op => {
      val outputSchema = findOutputSchema(dataLineage, op)
      Operation(None, Some(op.mainProps.id.toString), None, op.mainProps.name, reflectionToString(op), outputSchema)
    })
    val dataSources = dataLineage.operations.flatMap(op => op match {
      case r: Read => r.sources.map(s => DataSource(r.sourceType, s.path))
      case w: Write => Seq(DataSource(w.destinationType, w.path))
    })
    // progress for batch need to be generated during migration
    val progress = dataLineage.operations.find(_.isInstanceOf[BatchWrite]).map(_ => Progress(None, None, None, dataLineage.timestamp, -1))
    // TODO lineages are connected via meta dataset ids
    null
  }
}

