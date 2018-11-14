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

package za.co.absa.spline.persistence.mongo.dao

import com.mongodb.DBObject
import com.mongodb.casbah.query.Implicits.mongoQueryStatements
import za.co.absa.spline.persistence.api.CloseableIterable
import za.co.absa.spline.persistence.mongo.MongoConnection
import za.co.absa.spline.persistence.mongo.dao.BaselineLineageDAO.Component

import scala.concurrent.{ExecutionContext, Future}

class LineageDAOv3(override val connection: MongoConnection) extends BaselineLineageDAO {

  override val version: Int = 3

  override def upgrader: Option[VersionUpgrader] = None

  override protected def getMongoCollectionNameForComponent(component: Component): String = component.name

  override protected val overviewComponentFilter: PartialFunction[Component.SubComponent, DBObject] = {
    case Component.Operation =>
      "_typeHint" $in Seq(
        "za.co.absa.spline.model.op.Read",
        "za.co.absa.spline.model.op.Write")
  }

  override def getLineagesByPathAndInterval(path: String, start: Long, end: Long)(implicit ex: ExecutionContext): Future[CloseableIterable[LineageDBObject]] =
    Future.successful(new CloseableIterable[LineageDBObject](Iterable.empty.iterator, () => Unit))

}