from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when, concat, upper, lower, trim, regexp_replace, split, explode, array, struct, to_date, to_timestamp, year, month, day, hour, minute, second, date_format, datediff, months_between, add_months, date_add, date_sub, current_date, current_timestamp, sum as _sum, avg, count, min as _min, max as _max, stddev, variance, row_number, rank, dense_rank, lag, lead, first, last, collect_list, collect_set, approx_count_distinct, countDistinct, asc, desc, monotonically_increasing_id, spark_partition_id, sha2, md5, hash, abs, ceil, floor, round, sqrt, pow, exp, log, log10, sin, cos, tan, asin, acos, atan, degrees, radians, rand, randn, size, sort_array, array_contains, element_at, get_json_object, json_tuple, from_json, to_json, schema_of_json, broadcast, coalesce, repartition, partitionBy, bucketBy, sortBy, orderBy, asc_nulls_first, asc_nulls_last, desc_nulls_first, desc_nulls_last, udf, pandas_udf, PandasUDFType, broadcast as broadcast_func
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, FloatType, DoubleType, BooleanType, DateType, TimestampType, ArrayType, MapType, DecimalType
from pyspark.sql.window import Window
from pyspark.storagelevel import StorageLevel
import pandas as pd
from pyspark.ml.feature import Tokenizer, StopWordsRemover, CountVectorizer, IDF, StringIndexer, VectorAssembler, StandardScaler, MinMaxScaler, PCA, OneHotEncoder, Bucketizer, QuantileDiscretizer, Binarizer, NGram, ChiSqSelector, Normalizer
from pyspark.ml.classification import LogisticRegression, DecisionTreeClassifier, RandomForestClassifier, GBTClassifier, NaiveBayes, LinearSVC, MultilayerPerceptronClassifier, FMClassifier
from pyspark.ml.regression import LinearRegression, DecisionTreeRegressor, RandomForestRegressor, GBTRegressor, GeneralizedLinearRegression, IsotonicRegression
from pyspark.ml.clustering import KMeans, BisectingKMeans, GaussianMixture
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator, RegressionEvaluator, ClusteringEvaluator
from pyspark.ml.recommendation import ALS
from pyspark.ml.pipeline import Pipeline
from pyspark.ml.stat import Correlation, ChiSquareTest
from pyspark.sql import Row
from pyspark.sql.functions import pandas_udf, PandasUDFType
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("PySparkConcepts").master("local[*]").config("spark.sql.shuffle.partitions", "4").config("spark.sql.adaptive.enabled", "true").config("spark.sql.adaptive.coalescePartitions.enabled", "true").getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

data = [("Alice", 25, "Engineer", 75000), ("Bob", 30, "Manager", 90000), ("Charlie", 35, "Director", 120000), ("David", 28, "Engineer", 80000), ("Eve", 32, "Manager", 95000)]
columns = ["name", "age", "department", "salary"]
df = spark.createDataFrame(data, columns)

schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("salary", DoubleType(), True)
])
data_with_schema = [Row(1, "Alice", 25, 75000.0), Row(2, "Bob", 30, 90000.0)]
df_schema = spark.createDataFrame(data_with_schema, schema)

df.show()
df.printSchema()
df.describe().show()
df.columns
df.dtypes
df.count()
df.distinct().count()
df.head(3)
df.take(2)
df.first()
df.collect()
df.limit(3).show()

df.select("name", "age").show()
df.select(col("name"), col("age")).show()
df.select(df.name, df.age).show()
df.filter(df.age > 28).show()
df.filter(col("age") > 28).show()
df.filter("age > 28").show()
df.where(df.age > 28).show()
df.filter((df.age > 25) & (df.salary < 100000)).show()
df.filter((df.age > 25) | (df.salary > 100000)).show()
df.filter(df.age.isin(25, 30)).show()
df.filter(df.age.isNull()).show()
df.filter(df.age.isNotNull()).show()

df.withColumn("bonus", col("salary") * 0.1).show()
df.withColumn("name_upper", upper(col("name"))).show()
df.withColumn("name_lower", lower(col("name"))).show()
df.withColumn("name_trimmed", trim(col("name"))).show()
df.withColumn("full_name", concat(col("name"), lit(" "), col("department"))).show()
df.withColumn("age_group", when(col("age") < 30, "Young").when(col("age") < 35, "Middle").otherwise("Senior")).show()
df.withColumn("salary_category", when(col("salary") < 80000, "Low").when(col("salary") < 100000, "Medium").otherwise("High")).show()
df.withColumn("is_engineer", when(col("department") == "Engineer", True).otherwise(False)).show()
df.withColumnRenamed("name", "employee_name").show()
df.drop("age").show()
df.na.drop().show()
df.na.drop(subset=["age"]).show()
df.na.fill(0).show()
df.na.fill({"age": 0, "name": "Unknown"}).show()
df.na.replace("Engineer", "Software Engineer").show()

df.sort("age").show()
df.orderBy(col("age").asc()).show()
df.orderBy(col("salary").desc()).show()
df.orderBy(col("department").asc(), col("salary").desc()).show()

df.groupBy("department").count().show()
df.groupBy("department").agg(_sum("salary").alias("total_salary")).show()
df.groupBy("department").agg(avg("salary").alias("avg_salary")).show()
df.groupBy("department").agg(_min("salary").alias("min_salary"), _max("salary").alias("max_salary")).show()
df.groupBy("department").agg(count("*").alias("employee_count")).show()

df1 = spark.createDataFrame([("Alice", 25), ("Bob", 30)], ["name", "age"])
df2 = spark.createDataFrame([("Alice", 75000), ("Charlie", 120000)], ["name", "salary"])
df1.join(df2, "name", "inner").show()
df1.join(df2, "name", "left").show()
df1.join(df2, "name", "right").show()
df1.join(df2, "name", "outer").show()
df1.join(df2, "name", "left_semi").show()
df1.join(df2, "name", "left_anti").show()
df1.join(df2, df1.name == df2.name, "inner").show()

df.union(df).show()
df.unionAll(df).show()
df.intersect(df).show()
df.exceptAll(df).show()

window_spec = Window.partitionBy("department").orderBy(col("salary").desc())
df.withColumn("rank", rank().over(window_spec)).show()
df.withColumn("dense_rank", dense_rank().over(window_spec)).show()
df.withColumn("row_num", row_number().over(window_spec)).show()
df.withColumn("lag_salary", lag("salary", 1).over(window_spec)).show()
df.withColumn("lead_salary", lead("salary", 1).over(window_spec)).show()
df.withColumn("first_salary", first("salary").over(window_spec)).show()
df.withColumn("last_salary", last("salary").over(window_spec)).show()

df.withColumn("id", monotonically_increasing_id()).show()
df.withColumn("partition_id", spark_partition_id()).show()

df.persist(StorageLevel.MEMORY_ONLY)
df.unpersist()
df.cache()
df.unpersist()

df.explain()
df.explain(True)

df.write.mode("overwrite").csv("output.csv")
df.write.mode("overwrite").parquet("output.parquet")
df.write.mode("overwrite").json("output.json")
df.write.mode("overwrite").orc("output.orc")
df.write.mode("overwrite").format("avro").save("output.avro")

df_csv = spark.read.csv("data.csv", header=True, inferSchema=True)
df_parquet = spark.read.parquet("data.parquet")
df_json = spark.read.json("data.json")
df_orc = spark.read.orc("data.orc")

df.repartition(3).show()
df.coalesce(1).show()
df.repartition(col("department")).show()

array_data = [("Alice", [1, 2, 3]), ("Bob", [4, 5])]
array_df = spark.createDataFrame(array_data, ["name", "numbers"])
array_df.withColumn("array_size", size(col("numbers"))).show()
array_df.withColumn("exploded", explode(col("numbers"))).show()
array_df.withColumn("contains_2", array_contains(col("numbers"), 2)).show()
array_df.withColumn("first_element", element_at(col("numbers"), 1)).show()
array_df.withColumn("sorted_array", sort_array(col("numbers"))).show()

map_data = [("Alice", {"key1": "value1", "key2": "value2"}), ("Bob", {"key3": "value3"})]
map_df = spark.createDataFrame(map_data, ["name", "attributes"])
map_df.show()

json_data = [('{"name": "Alice", "age": 25}',), ('{"name": "Bob", "age": 30}',)]
json_df = spark.createDataFrame(json_data, ["json_string"])
json_df.withColumn("name", get_json_object(col("json_string"), "$.name")).show()
json_df.withColumn("data", from_json(col("json_string"), "STRUCT<name: STRING, age: INT>")).show()
json_df.withColumn("json", to_json(struct(col("json_string")))).show()

date_data = [("Alice", "2023-01-15"), ("Bob", "2023-02-20")]
date_df = spark.createDataFrame(date_data, ["name", "birth_date"])
date_df.withColumn("date", to_date(col("birth_date"))).show()
date_df.withColumn("year", year(col("date"))).show()
date_df.withColumn("month", month(col("date"))).show()
date_df.withColumn("day", day(col("date"))).show()
date_df.withColumn("formatted_date", date_format(col("date"), "yyyy-MM-dd")).show()
date_df.withColumn("current", current_date()).show()
date_df.withColumn("date_added", date_add(col("date"), 10)).show()
date_df.withColumn("date_subtracted", date_sub(col("date"), 5)).show()
date_df.withColumn("months_between", months_between(current_date(), col("date"))).show()
date_df.withColumn("month_added", add_months(col("date"), 3)).show()
date_df.withColumn("datediff", datediff(current_date(), col("date"))).show()

timestamp_data = [("Alice", "2023-01-15 10:30:00"), ("Bob", "2023-02-20 14:45:00")]
timestamp_df = spark.createDataFrame(timestamp_data, ["name", "timestamp"])
timestamp_df.withColumn("ts", to_timestamp(col("timestamp"))).show()
timestamp_df.withColumn("hour", hour(col("ts"))).show()
timestamp_df.withColumn("minute", minute(col("ts"))).show()
timestamp_df.withColumn("second", second(col("ts"))).show()
timestamp_df.withColumn("current_ts", current_timestamp()).show()

df.withColumn("salary_abs", abs(col("salary"))).show()
df.withColumn("salary_ceil", ceil(col("salary") / 1000)).show()
df.withColumn("salary_floor", floor(col("salary") / 1000)).show()
df.withColumn("salary_round", round(col("salary") / 1000)).show()
df.withColumn("salary_sqrt", sqrt(col("age"))).show()
df.withColumn("salary_pow", pow(col("age"), 2)).show()
df.withColumn("salary_exp", exp(col("age") / 10)).show()
df.withColumn("salary_log", log(col("salary"))).show()
df.withColumn("salary_log10", log10(col("salary"))).show()
df.withColumn("random", rand()).show()
df.withColumn("random_normal", randn()).show()

df.withColumn("name_hash", hash(col("name"))).show()
df.withColumn("name_md5", md5(col("name"))).show()
df.withColumn("name_sha2", sha2(col("name"), 256)).show()

string_data = [("Alice,25,Engineer",), ("Bob,30,Manager",)]
string_df = spark.createDataFrame(string_data, ["data"])
string_df.withColumn("split_data", split(col("data"), ",")).show()
string_df.withColumn("exploded", explode(split(col("data"), ","))).show()
string_df.withColumn("replaced", regexp_replace(col("data"), ",", "|")).show()

df.agg(approx_count_distinct("department")).show()
df.agg(countDistinct("department")).show()
df.agg(count("*").alias("total")).show()
df.agg(_sum("salary").alias("total_salary")).show()
df.agg(avg("salary").alias("avg_salary")).show()
df.agg(_min("salary").alias("min_salary")).show()
df.agg(_max("salary").alias("max_salary")).show()
df.agg(stddev("salary").alias("stddev_salary")).show()
df.agg(variance("salary").alias("variance_salary")).show()

df.createOrReplaceTempView("employees")
spark.sql("SELECT * FROM employees WHERE age > 28").show()
spark.sql("SELECT department, AVG(salary) as avg_salary FROM employees GROUP BY department").show()
spark.sql("SELECT department, COUNT(*) as count FROM employees GROUP BY department ORDER BY count DESC").show()
df.createGlobalTempView("global_employees")
spark.sql("SELECT * FROM global_temp.global_employees").show()

@udf(returnType=StringType())
def capitalize_name(name):
    return name.upper() if name else None

df.withColumn("capitalized_name", capitalize_name(col("name"))).show()

@pandas_udf("string")
def pandas_capitalize(names: pd.Series) -> pd.Series:
    return names.str.upper()

df.withColumn("pandas_capitalized", pandas_capitalize(col("name"))).show()

df.write.partitionBy("department").mode("overwrite").parquet("partitioned_data")
df.write.bucketBy(4, "department").sortBy("salary").mode("overwrite").saveAsTable("bucketed_table")

df1 = spark.createDataFrame([("Alice", 25), ("Bob", 30)], ["name", "age"])
df2 = spark.createDataFrame([("Alice", 75000), ("Bob", 90000)], ["name", "salary"])
broadcast_df1 = broadcast_func(df1)
broadcast_df1.join(df2, "name").show()

ml_data = [(1, "Hello World", 1.0), (2, "PySpark is great", 0.0), (3, "Machine Learning", 1.0)]
ml_columns = ["id", "text", "label"]
ml_df = spark.createDataFrame(ml_data, ml_columns)

tokenizer = Tokenizer(inputCol="text", outputCol="words")
words_data = tokenizer.transform(ml_df)
words_data.show()

remover = StopWordsRemover(inputCol="words", outputCol="filtered_words")
filtered_data = remover.transform(words_data)
filtered_data.show()

cv = CountVectorizer(inputCol="filtered_words", outputCol="raw_features")
cv_model = cv.fit(filtered_data)
cv_data = cv_model.transform(filtered_data)
cv_data.show()

idf = IDF(inputCol="raw_features", outputCol="features")
idf_model = idf.fit(cv_data)
idf_data = idf_model.transform(cv_data)
idf_data.show()

string_indexer = StringIndexer(inputCol="text", outputCol="text_index")
indexed_data = string_indexer.fit(ml_df).transform(ml_df)
indexed_data.show()

onehot_encoder = OneHotEncoder(inputCol="text_index", outputCol="text_vec")
onehot_data = onehot_encoder.fit(indexed_data).transform(indexed_data)
onehot_data.show()

assembler = VectorAssembler(inputCols=["text_index"], outputCol="features")
assembled_data = assembler.transform(indexed_data)
assembled_data.show()

scaler = StandardScaler(inputCol="features", outputCol="scaled_features")
scaled_data = scaler.fit(assembled_data).transform(assembled_data)
scaled_data.show()

minmax_scaler = MinMaxScaler(inputCol="features", outputCol="minmax_features")
minmax_data = minmax_scaler.fit(assembled_data).transform(assembled_data)
minmax_data.show()

pca = PCA(k=2, inputCol="features", outputCol="pca_features")
pca_model = pca.fit(assembled_data)
pca_data = pca_model.transform(assembled_data)
pca_data.show()

bucketizer = Bucketizer(splits=[0, 0.5, 1.0], inputCol="label", outputCol="bucketed")
bucketed_data = bucketizer.transform(ml_df)
bucketed_data.show()

discretizer = QuantileDiscretizer(numBuckets=2, inputCol="label", outputCol="discretized")
discretized_data = discretizer.fit(ml_df).transform(ml_df)
discretized_data.show()

binarizer = Binarizer(threshold=0.5, inputCol="label", outputCol="binarized")
binarized_data = binarizer.transform(ml_df)
binarized_data.show()

ngram = NGram(n=2, inputCol="words", outputCol="ngrams")
ngram_data = ngram.transform(words_data)
ngram_data.show()

chi_sq = ChiSqSelector(featuresCol="features", labelCol="label", outputCol="selected_features")
chi_sq_data = chi_sq.fit(idf_data).transform(idf_data)
chi_sq_data.show()

normalizer = Normalizer(inputCol="features", outputCol="norm_features")
normalized_data = normalizer.transform(idf_data)
normalized_data.show()

training_data, test_data = ml_df.randomSplit([0.8, 0.2])

lr = LogisticRegression(featuresCol="features", labelCol="label")
lr_model = lr.fit(assembled_data)
lr_predictions = lr_model.transform(assembled_data)
lr_predictions.show()

dt = DecisionTreeClassifier(featuresCol="features", labelCol="label")
dt_model = dt.fit(assembled_data)
dt_predictions = dt_model.transform(assembled_data)
dt_predictions.show()

rf = RandomForestClassifier(featuresCol="features", labelCol="label")
rf_model = rf.fit(assembled_data)
rf_predictions = rf_model.transform(assembled_data)
rf_predictions.show()

gbt = GBTClassifier(featuresCol="features", labelCol="label")
gbt_model = gbt.fit(assembled_data)
gbt_predictions = gbt_model.transform(assembled_data)
gbt_predictions.show()

nb = NaiveBayes(featuresCol="features", labelCol="label")
nb_model = nb.fit(assembled_data)
nb_predictions = nb_model.transform(assembled_data)
nb_predictions.show()

svm = LinearSVC(featuresCol="features", labelCol="label")
svm_model = svm.fit(assembled_data)
svm_predictions = svm_model.transform(assembled_data)
svm_predictions.show()

regression_data = [(1, 25, 75000), (2, 30, 90000), (3, 35, 120000), (4, 28, 80000)]
reg_columns = ["id", "age", "salary"]
reg_df = spark.createDataFrame(regression_data, reg_columns)

reg_assembler = VectorAssembler(inputCols=["age"], outputCol="features")
reg_assembled = reg_assembler.transform(reg_df)

linear_reg = LinearRegression(featuresCol="features", labelCol="salary")
linear_reg_model = linear_reg.fit(reg_assembled)
linear_reg_predictions = linear_reg_model.transform(reg_assembled)
linear_reg_predictions.show()

dt_reg = DecisionTreeRegressor(featuresCol="features", labelCol="salary")
dt_reg_model = dt_reg.fit(reg_assembled)
dt_reg_predictions = dt_reg_model.transform(reg_assembled)
dt_reg_predictions.show()

rf_reg = RandomForestRegressor(featuresCol="features", labelCol="salary")
rf_reg_model = rf_reg.fit(reg_assembled)
rf_reg_predictions = rf_reg_model.transform(reg_assembled)
rf_reg_predictions.show()

gbt_reg = GBTRegressor(featuresCol="features", labelCol="salary")
gbt_reg_model = gbt_reg.fit(reg_assembled)
gbt_reg_predictions = gbt_reg_model.transform(reg_assembled)
gbt_reg_predictions.show()

glr = GeneralizedLinearRegression(featuresCol="features", labelCol="salary")
glr_model = glr.fit(reg_assembled)
glr_predictions = glr_model.transform(reg_assembled)
glr_predictions.show()

iso = IsotonicRegression(featuresCol="features", labelCol="salary")
iso_model = iso.fit(reg_assembled)
iso_predictions = iso_model.transform(reg_assembled)
iso_predictions.show()

clustering_data = [(1, [1.0, 2.0]), (2, [2.0, 3.0]), (3, [3.0, 4.0]), (4, [4.0, 5.0])]
cluster_columns = ["id", "features"]
cluster_df = spark.createDataFrame(clustering_data, cluster_columns)

kmeans = KMeans(k=2, featuresCol="features")
kmeans_model = kmeans.fit(cluster_df)
kmeans_predictions = kmeans_model.transform(cluster_df)
kmeans_predictions.show()

bisecting_kmeans = BisectingKMeans(k=2, featuresCol="features")
bisecting_model = bisecting_kmeans.fit(cluster_df)
bisecting_predictions = bisecting_model.transform(cluster_df)
bisecting_predictions.show()

gmm = GaussianMixture(k=2, featuresCol="features")
gmm_model = gmm.fit(cluster_df)
gmm_predictions = gmm_model.transform(cluster_df)
gmm_predictions.show()

binary_evaluator = BinaryClassificationEvaluator(labelCol="label", metricName="areaUnderROC")
auc = binary_evaluator.evaluate(lr_predictions)
print(f"AUC: {auc}")

multi_evaluator = MulticlassClassificationEvaluator(labelCol="label", metricName="accuracy")
accuracy = multi_evaluator.evaluate(lr_predictions)
print(f"Accuracy: {accuracy}")

reg_evaluator = RegressionEvaluator(labelCol="salary", metricName="rmse")
rmse = reg_evaluator.evaluate(linear_reg_predictions)
print(f"RMSE: {rmse}")

cluster_evaluator = ClusteringEvaluator(featuresCol="features")
silhouette = cluster_evaluator.evaluate(kmeans_predictions)
print(f"Silhouette: {silhouette}")

als_data = [(1, 1, 5.0), (1, 2, 3.0), (2, 1, 4.0), (2, 2, 2.0)]
als_columns = ["user", "item", "rating"]
als_df = spark.createDataFrame(als_data, als_columns)

als = ALS(userCol="user", itemCol="item", ratingCol="rating")
als_model = als.fit(als_df)
als_predictions = als_model.transform(als_df)
als_predictions.show()

pipeline = Pipeline(stages=[tokenizer, remover, cv, idf])
pipeline_model = pipeline.fit(ml_df)
pipeline_data = pipeline_model.transform(ml_df)
pipeline_data.show()

correlation_data = [(1.0, 2.0), (2.0, 3.0), (3.0, 4.0)]
correlation_columns = ["feature1", "feature2"]
correlation_df = spark.createDataFrame(correlation_data, correlation_columns)

cor_assembler = VectorAssembler(inputCols=["feature1", "feature2"], outputCol="features")
cor_assembled = cor_assembler.transform(correlation_df)

correlation_matrix = Correlation.corr(cor_assembled, "features").head()
print(f"Correlation Matrix: {correlation_matrix}")

chi_square_data = [(1, [1.0, 2.0, 3.0], 0), (2, [2.0, 3.0, 4.0], 1)]
chi_square_columns = ["id", "features", "label"]
chi_square_df = spark.createDataFrame(chi_square_data, chi_square_columns)

chi_result = ChiSquareTest.test(chi_square_df, "features", "label")
chi_result.show()

df.withColumn("collected_list", collect_list("name").over(Window.partitionBy())).show()
df.withColumn("collected_set", collect_set("name").over(Window.partitionBy())).show()

df.select("department").distinct().show()
df.select("department").dropDuplicates().show()

df.sample(withReplacement=False, fraction=0.5).show()
df.sampleBy("department", fractions={"Engineer": 0.5, "Manager": 0.5}).show()

df.filter(col("salary") > 80000).randomSplit([0.7, 0.3])

df.stat.approxQuantile("salary", [0.25, 0.5, 0.75], 0.05)
df.stat.crosstab("department", "age").show()
df.stat.freqItems(["department"]).show()
df.stat.sampleBy("department", fractions={"Engineer": 0.5, "Manager": 0.5}).show()

df.foreach(lambda row: print(row.name))
df.foreachPartition(lambda iterator: [print(row) for row in iterator])

df.rdd.map(lambda row: (row.name, row.salary * 1.1)).take(5)
df.rdd.filter(lambda row: row.age > 28).collect()
df.rdd.reduce(lambda a, b: a if a.salary > b.salary else b)
df.rdd.groupBy(lambda row: row.department).mapValues(len).collect()

rdd = spark.sparkContext.parallelize([1, 2, 3, 4, 5])
rdd.map(lambda x: x * 2).collect()
rdd.filter(lambda x: x % 2 == 0).collect()
rdd.flatMap(lambda x: [x, x * 2]).collect()
rdd.reduce(lambda a, b: a + b)
rdd.count()
rdd.take(3)
rdd.first()
rdd.collect()
rdd.distinct().collect()
rdd.groupBy(lambda x: x % 2).mapValues(list).collect()
rdd.reduceByKey(lambda a, b: a + b).collect()
rdd.aggregate(0, lambda acc, x: acc + x, lambda acc1, acc2: acc1 + acc2)
rdd.fold(0, lambda acc, x: acc + x)

pair_rdd = spark.sparkContext.parallelize([("a", 1), ("b", 2), ("a", 3)])
pair_rdd.reduceByKey(lambda a, b: a + b).collect()
pair_rdd.groupByKey().mapValues(list).collect()
pair_rdd.sortByKey().collect()
pair_rdd.mapValues(lambda x: x * 2).collect()
pair_rdd.flatMapValues(lambda x: [x, x * 2]).collect()
pair_rdd.keys().collect()
pair_rdd.values().collect()
pair_rdd.join(pair_rdd).collect()
pair_rdd.leftOuterJoin(pair_rdd).collect()
pair_rdd.rightOuterJoin(pair_rdd).collect()
pair_rdd.cogroup(pair_rdd).mapValues(lambda x: (list(x[0]), list(x[1]))).collect()

rdd.persist(StorageLevel.MEMORY_ONLY)
rdd.unpersist()
rdd.cache()
rdd.unpersist()

rdd.repartition(4)
rdd.coalesce(2)

rdd.saveAsTextFile("rdd_output.txt")

spark.stop()
