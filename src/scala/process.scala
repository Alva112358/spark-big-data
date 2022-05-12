// Launch command :
// spark-shell 
//  --deploy-mode client 
//  --driver-java-options=-Dscala.color 
//  --conf spark.kryoserializer.buffer.max=256 
//  --executor-cores 10 
//  --executor-memory 10G 
//  --driver-memory 5g

// Please run the following code in sequence in spark-shell.
// If you need the data and pretrained model, please send email
// via junhua.liang@nyu.edu

import org.apache.spark.ml.feature.{Word2Vec, Word2VecModel}

// STEP 1 => APPLY WORD2VEC.
val model = Word2VecModel.load("model/word2vec")

val df = spark.read.format("json").load("loudacre/yelp")

val reviews = df.select(col("text"))

val reviews_arr = reviews.select(split(col("text"), " ").as("filteredPhrases"))

val result = model.transform(reviews_arr)

val source_df = df.withColumn("rid", monotonically_increasing_id())

val target_df = result.withColumn("rid", monotonically_increasing_id())

var res = source_df.as("df1")
    .join(target_df.as("df2"), source_df("rid") === target_df("rid"))
    .select("df1.user_id", "df1.business_id", "df1.review_id", "df2.word2vec", "df1.stars", "df1.date")


// STEP 2 => TO_UNIX_TIMESTAMP.
val res2 = res.withColumn("timestamp", unix_timestamp(col("date")))

// STEP 3 => SAVE FILES.
res2.write.format("parquet").mode("overwrite").option("compression", "snappy").save("yelp")



val res = spark.read.format("parquet").load("yelp_preprocess/user_info")
val bidx = spark.read.format("parquet").load("yelp_preprocess/business_index")
val r1 = res.join(bidx, res("item1_id") === bidx("business_index")).drop("item1_id").drop("business_index").withColumnRenamed("business_id", "r1")
val r2 = r1.join(bidx, r1("item2_id") === bidx("business_index")).drop("item2_id").drop("business_index").withColumnRenamed("business_id", "r2")
val r3 = r2.join(bidx, r2("item3_id") === bidx("business_index")).drop("item3_id").drop("business_index").withColumnRenamed("business_id", "r3")
val r4 = r3.join(bidx, r3("item4_id") === bidx("business_index")).drop("item4_id").drop("business_index").withColumnRenamed("business_id", "r4")
val r5 = r4.join(bidx, r4("item5_id") === bidx("business_index")).drop("item5_id").drop("business_index").withColumnRenamed("business_id", "r5")
val yelp_business = spark.read.format("json").load("yelp_origin/yelp_academic_dataset_business.json").select("business_id", "name", "categories")
val rr1 = r5.join(yelp_business, r5("r1") === yelp_business("business_id")).drop("r1").drop("business_id").withColumnRenamed("name", "r1_name").withColumnRenamed("categories", "r1_categories")
val rr2 = rr1.join(yelp_business, rr1("r2") === yelp_business("business_id")).drop("r2").drop("business_id").withColumnRenamed("name", "r2_name").withColumnRenamed("categories", "r2_categories")
val rr3 = rr2.join(yelp_business, rr2("r3") === yelp_business("business_id")).drop("r3").drop("business_id").withColumnRenamed("name", "r3_name").withColumnRenamed("categories", "r3_categories")
val rr4 = rr3.join(yelp_business, rr3("r4") === yelp_business("business_id")).drop("r4").drop("business_id").withColumnRenamed("name", "r4_name").withColumnRenamed("categories", "r4_categories")
val rr5 = rr4.join(yelp_business, rr4("r5") === yelp_business("business_id")).drop("r5").drop("business_id").withColumnRenamed("name", "r5_name").withColumnRenamed("categories", "r5_categories")

val review = spark.read.format("json").load("loudacre/yelp")

val benny_review = review.filter('user_id === "HP9Mhl8O91NPwbzng6WXUg")

benny_review.select("business_id").show(false)

/*
+----------------------+                                                        
|business_id           |
+----------------------+
|2jsjjTif9xrMrTe_89hkTw|
|fchpEk-UJf_AUz-51LEidg|
|J1XtshftqiJDuWW1hnbs9w|
+----------------------+
*/


val yelp_business = spark.read.format("json").load("yelp_origin/yelp_academic_dataset_business.json").select("business_id", "name", "categories")

yelp_business.filter('business_id === "2jsjjTif9xrMrTe_89hkTw").show(false)

yelp_business.filter('business_id === "fchpEk-UJf_AUz-51LEidg").show(false)

/*
+----------------------+                                                        
|business_id           |
+----------------------+
|fitSn2LBLb5OXV7hdl86Kw|
|cuYMRqzehT24XJk-YSrExw|
|gz9b3mEPMIAoWVj6n5Wccw|
|I6Hn7rqEX-KXcoB6BIMslA|
|vRtw_YYSwxszxonseZhWCQ|
+----------------------+
*/

