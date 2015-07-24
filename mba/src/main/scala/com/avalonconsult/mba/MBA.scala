package com.avalonconsult.mba

import com.couchbase.client.java.document.JsonDocument
import com.couchbase.client.java.document.json.{JsonArray, JsonObject}
import com.couchbase.client.java.view.ViewQuery
import com.couchbase.spark._
import org.apache.spark.{SparkContext, SparkConf}
import scala.collection.JavaConverters._
import scala.collection.JavaConversions._

/**
 * Created by kruthar on 7/23/15.
 */
object MBA {
  def main(args : Array[String]): Unit = {
    // Create a Spark Configuration with all the settings we will need to connect to Couchbase including a list of
    // Couchbase cluster nodes and a bucket.
    // NOTE: we are setting the mast to local[*] which means we will run this Spark job locally instead of on a cluster.
    val sparkConf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("MBA")
      .set("com.couchbase.nodes", "localhost")
      .set("com.couchbase.bucket.retail", "")

    val sc = new SparkContext(sparkConf)

    // Retrieve all of our order documents by querying the allOrders View in Couchbase.
    val order_docs = sc.couchbaseView(ViewQuery.from("retail", "allOrders"))
      .map(row => row.id)
      .couchbaseGet[JsonDocument]()

    // For each order, use a flatMap explode out all of the items, then count the number of appearances of each item.
    val purchase_counts = order_docs
      .flatMap(doc => for {
        item <- doc.content().getArray("items").asScala
      } yield (item.asInstanceOf[Integer], 1))
      .reduceByKey((a, b) => a + b)

    // For each document, create tuples from matching up each item of the order with each other item in the order. Then
    // we want to take all of the product pairs and reduce them to get a sum for each unique pairing, we do this
    // with the reduceByKey method.
    // NOTE: the if statement in the for comprehension is only allowing pairs where item1 is less then item2, this will
    // filter out all duplicates and prevent matching items with themselves.
    val product_pairs = order_docs
      .flatMap(doc => for {
        item1 <- doc.content().getArray("items").asScala
        item2 <- doc.content().getArray("items").asScala
        if item1.asInstanceOf[Integer] <  item2.asInstanceOf[Integer]
      } yield ((item1.asInstanceOf[Integer], item2.asInstanceOf[Integer]), 1))
      .reduceByKey((a, b) => a + b)

    // Next we want to use flatMap to create two tuples for each pair sum, each tuple is one
    // direction of the product pair. For example if we gathered the sum for a product pair like this: ((35, 36), 5) where
    // (35, 36) is the product pair and 5 is the sum, then we would want to create two tuples: (35, (36, 5)) and (36, (35, 5)).
    // After creating a list of bidirectional product pairs, use the groupByKey so that for each product ID we have a
    // list of other product IDs and affinities that they were purchased together. Then sort that list and grab the
    // best 3 results for each product ID, while at the same time gathering the total number of purchases of this product.
    // Lastly, join with our purchase_counts.
    val recommendations = product_pairs
      .flatMap(tuple => List((tuple._1._1, (tuple._1._2, tuple._2)), (tuple._1._2, (tuple._1._1, tuple._2))))
      .groupByKey()
      .map(product => (product._1, product._2.toList.sortBy(l => -l._2).take(3)))
      .join(purchase_counts)

    // Take our final data set and convert the tuples into JsonObjects to be stored in Couchbase.
    val documents = recommendations
      .map(recommendation => {
        val total_purchases = recommendation._2._2.toDouble
        val json_items: java.util.List[JsonObject] = for {
          item <- recommendation._2._1
        } yield JsonObject.empty().put("product", item._1).put("affinity", item._2 / total_purchases)
        ("product_recommendation::" + recommendation._1, JsonObject.empty().put("type", "product_recommendation").put("recommendations", JsonArray.from(json_items)))
      })

    documents.toCouchbaseDocument[JsonDocument].saveToCouchbase("retail")
  }

}
