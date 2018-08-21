/*
 * Copyright 2017 Barclays Africa Group Limited
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

package za.co.absa.spline.core.transformations

import java.util.UUID
import java.util.UUID.randomUUID

import org.apache.spark
import org.scalatest.mockito.MockitoSugar
import org.scalatest.{AsyncFunSpec, Matchers}
import za.co.absa.spline.core.transformations.LineageProjectionMerger.mergeProjections
import za.co.absa.spline.model.dt.Simple
import za.co.absa.spline.model.expr._
import za.co.absa.spline.model.op.{Join, Operation, OperationProps, Projection}
import za.co.absa.spline.model.{Attribute, DataLineage, MetaDataset, Schema}

import scala.collection.mutable

class LineageProjectionMergerSpec extends AsyncFunSpec with Matchers with MockitoSugar {

  describe("consolidateReferences() method") {

    it("should filter out unused references") {
      val attributeType = Simple("type", nullable = false)

      val attributes = Seq(
        Attribute(randomUUID, "a", attributeType.id),
        Attribute(randomUUID, "b", attributeType.id),
        Attribute(randomUUID, "c", attributeType.id),
        Attribute(randomUUID, "d", attributeType.id)
      )

      val datasets = Seq(
        MetaDataset(randomUUID, Schema(Seq(attributes(0).id, attributes(1).id))),
        MetaDataset(randomUUID, Schema(Seq(attributes(1).id))),
        MetaDataset(randomUUID, Schema(Seq(attributes(0).id))),
        MetaDataset(randomUUID, Schema(Seq(attributes(2).id, attributes(3).id)))
      )

      val operations = Seq(
        Projection(
          OperationProps(
            randomUUID,
            "node2",
            Seq(datasets(1).id),
            datasets(2).id),
          Seq.empty),
        Projection(
          OperationProps(
            randomUUID,
            "node1",
            Seq(datasets(0).id),
            datasets(1).id),
          Seq.empty)
      )

      val lineage = DataLineage(
        "appId",
        "appName",
        1L,
        spark.SPARK_VERSION,
        operations,
        datasets,
        attributes,
        Seq(attributeType)
      )

      val expectedLineage = lineage.copy(datasets = Seq(datasets(0), datasets(1), datasets(2)), attributes = Seq(attributes(0), attributes(1)))

      LineageProjectionMerger.cleanupReferences(lineage) shouldEqual expectedLineage
    }
  }

  describe("mergeProjections() method") {

    val aType = Simple("type", nullable = true)

    val emptyLineage = DataLineage("", "", -1, spark.SPARK_VERSION, Seq(mock[Operation]), Seq(mock[MetaDataset]), Nil, Nil)

    def createGenericExpressions(names: String*): Seq[Expression] = {
      names.map(Generic(aType.id, Seq.empty, _, None))
    }

    def createCompositeExpressions(attributeNames: (String, String)*)
                                  (attrCollector: mutable.Map[UUID, Attribute]): Seq[Expression] = {
      attributeNames.map({ case (name1, name2) =>
        val refAttrId = attrCollector.
          find { case (_, attr) => attr.name == name1 }.
          map { case (id, _) => id }.
          getOrElse {
            val id = randomUUID
            attrCollector.put(id, Attribute(id, name1, aType.id))
            id
          }
        Alias(name2, AttrRef(refAttrId))
      })
    }

    it("should join two compatible projections into one node") {

      val metaDatasetId = randomUUID
      val outputMetaDataset = randomUUID

      val inputNodes = Seq(
        Projection(
          OperationProps(
            randomUUID,
            "node2",
            Seq(metaDatasetId),
            outputMetaDataset),
          createGenericExpressions("c", "d")),
        Projection(
          OperationProps(
            randomUUID,
            "node1",
            Seq.empty,
            metaDatasetId),
          createGenericExpressions("a", "b"))
      )

      val expectedNodes = Seq(Projection(
        OperationProps(
          id = null,
          "node1",
          Seq.empty,
          outputMetaDataset),
        createGenericExpressions("a", "b", "c", "d")))

      val result = mergeProjections(emptyLineage.copy(operations = inputNodes))

      result.operations.map(_.updated(_.copy(id = null))) shouldEqual expectedNodes
    }

    it("should join three compatible projections into one node") {
      val metaDataset1Id = randomUUID
      val metaDataset2Id = randomUUID
      val outputMetaDataset = randomUUID

      val inputNodes = Seq(
        Projection(
          OperationProps(
            randomUUID,
            "node3",
            Seq(metaDataset2Id),
            outputMetaDataset),
          createGenericExpressions("e", "f")),
        Projection(
          OperationProps(
            randomUUID,
            "node2",
            Seq(metaDataset1Id),
            metaDataset2Id),
          createGenericExpressions("c", "d")),
        Projection(
          OperationProps(
            randomUUID,
            "node1",
            Seq.empty,
            metaDataset1Id),
          createGenericExpressions("a", "b"))
      )

      val expectedNodes = Seq(Projection(
        OperationProps(
          id = null,
          "node1",
          Seq.empty,
          outputMetaDataset),
        createGenericExpressions("a", "b", "c", "d", "e", "f"))
      )

      val result = mergeProjections(emptyLineage.copy(operations = inputNodes))

      result.operations.map(_.updated(_.copy(id = null))) shouldEqual expectedNodes
    }

    it("should not merge incompatible projections") {
      val metaDatasetId = randomUUID
      val outputMetaDataset = randomUUID
      val attrs = mutable.Map.empty[UUID, Attribute]

      val input = Seq(
        Projection(
          OperationProps(
            randomUUID,
            "node2",
            Seq(metaDatasetId),
            outputMetaDataset),
          createCompositeExpressions(("b", "c"))(attrs)),
        Projection(
          OperationProps(
            randomUUID,
            "node1",
            Seq(),
            metaDatasetId),
          createCompositeExpressions(("a", "b"))(attrs))
      )

      val result = mergeProjections(emptyLineage.copy(operations = input, attributes = attrs.values.toSeq))

      result.operations shouldEqual input
    }

    it("should merge a branch of compatible projections within a diamond graph") {
      val metaDatasetRootId = randomUUID
      val metaDatasetAnotherBranchId = randomUUID
      val metaDatasetBranch1Id = randomUUID
      val metaDatasetBranch2Id = randomUUID
      val metaDatasetBranch3Id = randomUUID
      val outputMetaDataset = randomUUID

      val inputNodes = Seq(
        Join(
          OperationProps(
            randomUUID,
            "join",
            Seq(metaDatasetAnotherBranchId, metaDatasetBranch3Id),
            outputMetaDataset),
          None,
          "inner"),
        Projection(
          OperationProps(
            randomUUID,
            "branch3",
            Seq(metaDatasetBranch2Id),
            metaDatasetBranch3Id),
          createGenericExpressions("e", "f")),
        Projection(
          OperationProps(
            randomUUID,
            "branch2",
            Seq(metaDatasetBranch1Id),
            metaDatasetBranch2Id),
          createGenericExpressions("c", "d")),
        Projection(
          OperationProps(
            randomUUID,
            "branch1",
            Seq(metaDatasetRootId),
            metaDatasetBranch1Id),
          createGenericExpressions("a", "b")),
        Projection(
          OperationProps(
            randomUUID,
            "anotherBranch",
            Seq(metaDatasetRootId),
            metaDatasetAnotherBranchId),
          createGenericExpressions("n")),
        Projection(
          OperationProps(
            randomUUID,
            "root",
            Seq.empty,
            metaDatasetRootId),
          createGenericExpressions("r"))
      )

      val expectedNodes = Seq(
        Join(
          OperationProps(
            id = null,
            "join",
            Seq(metaDatasetAnotherBranchId, metaDatasetBranch3Id),
            outputMetaDataset),
          None,
          "inner"),
        Projection(
          OperationProps(
            id = null,
            "branch1",
            Seq(metaDatasetRootId),
            metaDatasetBranch3Id),
          createGenericExpressions("a", "b", "c", "d", "e", "f")),
        Projection(
          OperationProps(
            id = null,
            "anotherBranch",
            Seq(metaDatasetRootId),
            metaDatasetAnotherBranchId),
          createGenericExpressions("n")),
        Projection(
          OperationProps(
            id = null,
            "root",
            Seq.empty,
            metaDatasetRootId),
          createGenericExpressions("r"))
      )

      val result = mergeProjections(emptyLineage.copy(operations = inputNodes))

      result.operations.map(_.updated(_.copy(id = null))) shouldEqual expectedNodes
    }


    it("should merge a graph with two branches of projections. Only the branch of compatible nodes should be merged") {
      val metaDatasetRootId = randomUUID
      val metaDatasetCompatible1Id = randomUUID
      val metaDatasetCompatible2Id = randomUUID
      val metaDatasetIncompatible1Id = randomUUID
      val metaDatasetIncompatible2Id = randomUUID
      val outputMetaDataset = randomUUID
      val attrs = mutable.Map.empty[UUID, Attribute]

      val inputNodes = Seq(
        Join(
          OperationProps(
            randomUUID,
            "join",
            Seq(metaDatasetCompatible2Id, metaDatasetIncompatible2Id),
            outputMetaDataset),
          None,
          "inner"),
        Projection(
          OperationProps(
            randomUUID,
            "incompatible2",
            Seq(metaDatasetIncompatible1Id),
            metaDatasetIncompatible2Id),
          createCompositeExpressions(("b", "c"))(attrs)),
        Projection(
          OperationProps(
            randomUUID,
            "incompatible1",
            Seq(metaDatasetRootId),
            metaDatasetIncompatible1Id),
          createCompositeExpressions(("a", "b"))(attrs)),
        Projection(
          OperationProps(
            randomUUID,
            "compatible2",
            Seq(metaDatasetCompatible1Id),
            metaDatasetCompatible2Id),
          createCompositeExpressions(("a", "b"))(attrs)),
        Projection(
          OperationProps(
            randomUUID,
            "compatible1",
            Seq(metaDatasetRootId),
            metaDatasetCompatible1Id),
          createCompositeExpressions(("b", "c"))(attrs)),
        Projection(
          OperationProps(
            randomUUID,
            "root",
            Seq.empty,
            metaDatasetRootId),
          createGenericExpressions("r"))
      )

      val expectedNodes = Seq(
        Join(
          OperationProps(
            id = null,
            "join",
            Seq(metaDatasetCompatible2Id, metaDatasetIncompatible2Id),
            outputMetaDataset),
          None,
          "inner"),
        Projection(
          OperationProps(
            id = null,
            "incompatible2",
            Seq(metaDatasetIncompatible1Id),
            metaDatasetIncompatible2Id),
          createCompositeExpressions(("b", "c"))(attrs)),
        Projection(
          OperationProps(
            id = null,
            "incompatible1",
            Seq(metaDatasetRootId),
            metaDatasetIncompatible1Id),
          createCompositeExpressions(("a", "b"))(attrs)),
        Projection(
          OperationProps(
            id = null,
            "compatible1",
            Seq(metaDatasetRootId),
            metaDatasetCompatible2Id),
          createCompositeExpressions(("b", "c"), ("a", "b"))(attrs)),
        Projection(
          OperationProps(
            id = null,
            "root",
            Seq.empty,
            metaDatasetRootId),
          createGenericExpressions("r"))
      )

      val result = mergeProjections(emptyLineage.copy(operations = inputNodes, attributes = attrs.values.toSeq))

      result.operations.map(_.updated(_.copy(id = null))) shouldEqual expectedNodes
    }
  }
}
