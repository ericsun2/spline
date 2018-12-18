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


import com.outr.arango._
import com.outr.arango.managed._
import org.scalatest.mockito.MockitoSugar
import org.scalatest.{FunSpec, Matchers}

import scala.concurrent.Await
import scala.concurrent.duration.Duration

class ScarangoTest extends FunSpec with Matchers with MockitoSugar {

  describe("scarango") {
    it("funspec") {
      case class Fruit(name: String,
                 _key: Option[String] = None,
                 _id: Option[String] = None,
                 _rev: Option[String] = None) extends DocumentOption
      case class Progress(_id: Option[String], _key: Option[String], _rev: Option[String], timestamp: Long, readCount: Long) extends DocumentOption
      case class Execution(_id: Option[String], _key: Option[String], _rev: Option[String], appId: String, appName: String, sparkVer: String, timestamp: Long) extends DocumentOption
      case class Operation(_id: Option[String], _key: Option[String], _rev: Option[String], name: String, expression: String)
      case class DataSource(_id: Option[String], _key: Option[String], _rev: Option[String], name: String, path: String)
      object Database extends Graph("lineages") {
        val progress: VertexCollection[Progress] = vertex[Progress]("progress")
        val execution: VertexCollection[Execution] = vertex[Execution]("execution")
        val operation: VertexCollection[Operation] = vertex[Operation]("operation")
        val dataSource: VertexCollection[DataSource] = vertex[DataSource]("dataSource")
      }
      val result = Await.result(Database.init(), Duration.Inf)
      println("Init result: " + result)
//      print("Graph creation result: " + Await.result(Database.fruit.create(), Duration.Inf))
//      println(Await.result(Database.fruit.insert(Fruit("Apple")), Duration.Inf))
      val query = aql"FOR f IN fruit RETURN f"
//      println(Await.result(Database.fruit.cursor(query), Duration.Inf))
    }
  }
}

