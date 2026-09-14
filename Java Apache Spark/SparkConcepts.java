import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.functions.*;
import org.apache.spark.sql.types.*;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.*;
import org.apache.spark.ml.feature.*;
import org.apache.spark.ml.classification.*;
import org.apache.spark.ml.regression.*;
import org.apache.spark.ml.clustering.*;
import org.apache.spark.ml.evaluation.*;
import org.apache.spark.ml.recommendation.ALS;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.stat.Correlation;
import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.ml.linalg.VectorUDT;
import scala.Tuple2;
import scala.Tuple3;
import java.util.*;
import java.util.stream.Collectors;

public class SparkConcepts {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("SparkConcepts")
                .master("local[*]")
                .config("spark.sql.shuffle.partitions", "4")
                .config("spark.sql.adaptive.enabled", "true")
                .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
                .getOrCreate();

        spark.sparkContext().setLogLevel("ERROR");

        List<Row> data = Arrays.asList(
                RowFactory.create("Alice", 25, "Engineer", 75000.0),
                RowFactory.create("Bob", 30, "Manager", 90000.0),
                RowFactory.create("Charlie", 35, "Director", 120000.0),
                RowFactory.create("David", 28, "Engineer", 80000.0),
                RowFactory.create("Eve", 32, "Manager", 95000.0)
        );

        StructType schema = new StructType(new StructField[]{
                new StructField("name", DataTypes.StringType, false, Metadata.empty()),
                new StructField("age", DataTypes.IntegerType, false, Metadata.empty()),
                new StructField("department", DataTypes.StringType, false, Metadata.empty()),
                new StructField("salary", DataTypes.DoubleType, false, Metadata.empty())
        });

        Dataset<Row> df = spark.createDataFrame(data, schema);

        df.show();
        df.printSchema();
        df.describe().show();
        df.columns();
        df.dtypes();
        df.count();
        df.distinct().count();
        df.head(3);
        df.take(2);
        df.first();
        df.collect();
        df.limit(3).show();

        df.select("name", "age").show();
        df.select(col("name"), col("age")).show();
        df.select(df.col("name"), df.col("age")).show();
        df.filter(df.col("age").gt(28)).show();
        df.filter(col("age").gt(28)).show();
        df.filter("age > 28").show();
        df.where(df.col("age").gt(28)).show();
        df.filter(df.col("age").gt(25).and(df.col("salary").lt(100000))).show();
        df.filter(df.col("age").gt(25).or(df.col("salary").gt(100000))).show();
        df.filter(df.col("age").isin(25, 30)).show();
        df.filter(df.col("age").isNull()).show();
        df.filter(df.col("age").isNotNull()).show();

        df.withColumn("bonus", col("salary").multiply(0.1)).show();
        df.withColumn("name_upper", upper(col("name"))).show();
        df.withColumn("name_lower", lower(col("name"))).show();
        df.withColumn("name_trimmed", trim(col("name"))).show();
        df.withColumn("full_name", concat(col("name"), lit(" "), col("department"))).show();
        df.withColumn("age_group", when(col("age").lt(30), "Young")
                .when(col("age").lt(35), "Middle")
                .otherwise("Senior")).show();
        df.withColumn("salary_category", when(col("salary").lt(80000), "Low")
                .when(col("salary").lt(100000), "Medium")
                .otherwise("High")).show();
        df.withColumn("is_engineer", when(col("department").equalTo("Engineer"), true).otherwise(false)).show();
        df.withColumnRenamed("name", "employee_name").show();
        df.drop("age").show();
        df.na().drop().show();
        df.na().drop(new String[]{"age"}).show();
        df.na().fill(0).show();
        df.na().fill(0, new String[]{"age"}).show();
        df.na().fill("Unknown", new String[]{"name"}).show();
        df.na().replace("department", new String[]{"Engineer"}, new String[]{"Software Engineer"}).show();

        df.sort("age").show();
        df.orderBy(col("age").asc()).show();
        df.orderBy(col("salary").desc()).show();
        df.orderBy(col("department").asc(), col("salary").desc()).show();

        df.groupBy("department").count().show();
        df.groupBy("department").agg(sum("salary").alias("total_salary")).show();
        df.groupBy("department").agg(avg("salary").alias("avg_salary")).show();
        df.groupBy("department").agg(min("salary").alias("min_salary"), max("salary").alias("max_salary")).show();
        df.groupBy("department").agg(count("*").alias("employee_count")).show();

        List<Row> data1 = Arrays.asList(
                RowFactory.create("Alice", 25),
                RowFactory.create("Bob", 30)
        );
        StructType schema1 = new StructType(new StructField[]{
                new StructField("name", DataTypes.StringType, false, Metadata.empty()),
                new StructField("age", DataTypes.IntegerType, false, Metadata.empty())
        });
        Dataset<Row> df1 = spark.createDataFrame(data1, schema1);

        List<Row> data2 = Arrays.asList(
                RowFactory.create("Alice", 75000.0),
                RowFactory.create("Charlie", 120000.0)
        );
        StructType schema2 = new StructType(new StructField[]{
                new StructField("name", DataTypes.StringType, false, Metadata.empty()),
                new StructField("salary", DataTypes.DoubleType, false, Metadata.empty())
        });
        Dataset<Row> df2 = spark.createDataFrame(data2, schema2);

        df1.join(df2, "name", "inner").show();
        df1.join(df2, "name", "left").show();
        df1.join(df2, "name", "right").show();
        df1.join(df2, "name", "outer").show();
        df1.join(df2, "name", "left_semi").show();
        df1.join(df2, "name", "left_anti").show();
        df1.join(df2, df1.col("name").equalTo(df2.col("name")), "inner").show();

        df.union(df).show();
        df.unionAll(df).show();
        df.intersect(df).show();
        df.exceptAll(df).show();

        WindowSpec windowSpec = Window.partitionBy("department").orderBy(col("salary").desc());
        df.withColumn("rank", rank().over(windowSpec)).show();
        df.withColumn("dense_rank", dense_rank().over(windowSpec)).show();
        df.withColumn("row_num", row_number().over(windowSpec)).show();
        df.withColumn("lag_salary", lag("salary", 1).over(windowSpec)).show();
        df.withColumn("lead_salary", lead("salary", 1).over(windowSpec)).show();
        df.withColumn("first_salary", first("salary").over(windowSpec)).show();
        df.withColumn("last_salary", last("salary").over(windowSpec)).show();

        df.withColumn("id", monotonically_increasing_id()).show();
        df.withColumn("partition_id", spark_partition_id()).show();

        df.persist(StorageLevel.MEMORY_ONLY());
        df.unpersist();
        df.cache();
        df.unpersist();

        df.explain();
        df.explain(true);

        df.write().mode("overwrite").csv("output.csv");
        df.write().mode("overwrite").parquet("output.parquet");
        df.write().mode("overwrite").json("output.json");
        df.write().mode("overwrite").orc("output.orc");

        Dataset<Row> dfCsv = spark.read().option("header", true).option("inferSchema", true).csv("data.csv");
        Dataset<Row> dfParquet = spark.read().parquet("data.parquet");
        Dataset<Row> dfJson = spark.read().json("data.json");
        Dataset<Row> dfOrc = spark.read().orc("data.orc");

        df.repartition(3);
        df.coalesce(1);
        df.repartition(col("department"));

        List<Row> arrayData = Arrays.asList(
                RowFactory.create("Alice", Arrays.asList(1, 2, 3)),
                RowFactory.create("Bob", Arrays.asList(4, 5))
        );
        StructType arraySchema = new StructType(new StructField[]{
                new StructField("name", DataTypes.StringType, false, Metadata.empty()),
                new StructField("numbers", DataTypes.createArrayType(DataTypes.IntegerType), false, Metadata.empty())
        });
        Dataset<Row> arrayDf = spark.createDataFrame(arrayData, arraySchema);

        arrayDf.withColumn("array_size", size(col("numbers"))).show();
        arrayDf.withColumn("exploded", explode(col("numbers"))).show();
        arrayDf.withColumn("contains_2", array_contains(col("numbers"), 2)).show();
        arrayDf.withColumn("first_element", element_at(col("numbers"), 1)).show();
        arrayDf.withColumn("sorted_array", sort_array(col("numbers"))).show();

        List<Row> mapData = Arrays.asList(
                RowFactory.create("Alice", Map.of("key1", "value1", "key2", "value2")),
                RowFactory.create("Bob", Map.of("key3", "value3"))
        );
        StructType mapSchema = new StructType(new StructField[]{
                new StructField("name", DataTypes.StringType, false, Metadata.empty()),
                new StructField("attributes", DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType), false, Metadata.empty())
        });
        Dataset<Row> mapDf = spark.createDataFrame(mapData, mapSchema);
        mapDf.show();

        List<Row> jsonData = Arrays.asList(
                RowFactory.create("{\"name\": \"Alice\", \"age\": 25}"),
                RowFactory.create("{\"name\": \"Bob\", \"age\": 30}")
        );
        StructType jsonSchema = new StructType(new StructField[]{
                new StructField("json_string", DataTypes.StringType, false, Metadata.empty())
        });
        Dataset<Row> jsonDf = spark.createDataFrame(jsonData, jsonSchema);

        jsonDf.withColumn("name", get_json_object(col("json_string"), "$.name")).show();
        jsonDf.withColumn("data", from_json(col("json_string"), 
                new StructType(new StructField[]{
                        new StructField("name", DataTypes.StringType, false, Metadata.empty()),
                        new StructField("age", DataTypes.IntegerType, false, Metadata.empty())
                }))).show();
        jsonDf.withColumn("json", to_json(struct(col("json_string")))).show();

        List<Row> dateData = Arrays.asList(
                RowFactory.create("Alice", "2023-01-15"),
                RowFactory.create("Bob", "2023-02-20")
        );
        StructType dateSchema = new StructType(new StructField[]{
                new StructField("name", DataTypes.StringType, false, Metadata.empty()),
                new StructField("birth_date", DataTypes.StringType, false, Metadata.empty())
        });
        Dataset<Row> dateDf = spark.createDataFrame(dateData, dateSchema);

        dateDf.withColumn("date", to_date(col("birth_date"))).show();
        dateDf.withColumn("year", year(col("date"))).show();
        dateDf.withColumn("month", month(col("date"))).show();
        dateDf.withColumn("day", dayofmonth(col("date"))).show();
        dateDf.withColumn("formatted_date", date_format(col("date"), "yyyy-MM-dd")).show();
        dateDf.withColumn("current", current_date()).show();
        dateDf.withColumn("date_added", date_add(col("date"), 10)).show();
        dateDf.withColumn("date_subtracted", date_sub(col("date"), 5)).show();
        dateDf.withColumn("months_between", months_between(current_date(), col("date"))).show();
        dateDf.withColumn("month_added", add_months(col("date"), 3)).show();
        dateDf.withColumn("datediff", datediff(current_date(), col("date"))).show();

        List<Row> timestampData = Arrays.asList(
                RowFactory.create("Alice", "2023-01-15 10:30:00"),
                RowFactory.create("Bob", "2023-02-20 14:45:00")
        );
        StructType timestampSchema = new StructType(new StructField[]{
                new StructField("name", DataTypes.StringType, false, Metadata.empty()),
                new StructField("timestamp", DataTypes.StringType, false, Metadata.empty())
        });
        Dataset<Row> timestampDf = spark.createDataFrame(timestampData, timestampSchema);

        timestampDf.withColumn("ts", to_timestamp(col("timestamp"))).show();
        timestampDf.withColumn("hour", hour(col("ts"))).show();
        timestampDf.withColumn("minute", minute(col("ts"))).show();
        timestampDf.withColumn("second", second(col("ts"))).show();
        timestampDf.withColumn("current_ts", current_timestamp()).show();

        df.withColumn("salary_abs", abs(col("salary"))).show();
        df.withColumn("salary_ceil", ceil(col("salary").divide(1000))).show();
        df.withColumn("salary_floor", floor(col("salary").divide(1000))).show();
        df.withColumn("salary_round", round(col("salary").divide(1000))).show();
        df.withColumn("salary_sqrt", sqrt(col("age").cast(DataTypes.DoubleType()))).show();
        df.withColumn("salary_pow", pow(col("age").cast(DataTypes.DoubleType()), 2)).show();
        df.withColumn("salary_exp", exp(col("age").divide(10))).show();
        df.withColumn("salary_log", log(col("salary"))).show();
        df.withColumn("salary_log10", log10(col("salary"))).show();
        df.withColumn("random", rand()).show();
        df.withColumn("random_normal", randn()).show();

        df.withColumn("name_hash", hash(col("name"))).show();
        df.withColumn("name_md5", md5(col("name"))).show();
        df.withColumn("name_sha2", sha2(col("name"), 256)).show();

        List<Row> stringData = Arrays.asList(
                RowFactory.create("Alice,25,Engineer"),
                RowFactory.create("Bob,30,Manager")
        );
        StructType stringSchema = new StructType(new StructField[]{
                new StructField("data", DataTypes.StringType, false, Metadata.empty())
        });
        Dataset<Row> stringDf = spark.createDataFrame(stringData, stringSchema);

        stringDf.withColumn("split_data", split(col("data"), ",")).show();
        stringDf.withColumn("exploded", explode(split(col("data"), ","))).show();
        stringDf.withColumn("replaced", regexp_replace(col("data"), ",", "\\|")).show();

        df.agg(approx_count_distinct("department")).show();
        df.agg(countDistinct("department")).show();
        df.agg(count("*").alias("total")).show();
        df.agg(sum("salary").alias("total_salary")).show();
        df.agg(avg("salary").alias("avg_salary")).show();
        df.agg(min("salary").alias("min_salary")).show();
        df.agg(max("salary").alias("max_salary")).show();
        df.agg(stddev("salary").alias("stddev_salary")).show();
        df.agg(variance("salary").alias("variance_salary")).show();

        df.createOrReplaceTempView("employees");
        spark.sql("SELECT * FROM employees WHERE age > 28").show();
        spark.sql("SELECT department, AVG(salary) as avg_salary FROM employees GROUP BY department").show();
        spark.sql("SELECT department, COUNT(*) as count FROM employees GROUP BY department ORDER BY count DESC").show();
        df.createGlobalTempView("global_employees");
        spark.sql("SELECT * FROM global_temp.global_employees").show();

        df.select("department").distinct().show();
        df.select("department").dropDuplicates().show();

        df.sample(false, 0.5).show();
        df.sampleBy("department", Map.of("Engineer", 0.5, "Manager", 0.5)).show();

        Dataset<Row>[] splits = df.filter(col("salary").gt(80000)).randomSplit(new double[]{0.7, 0.3});

        df.stat().approxQuantile("salary", new double[]{0.25, 0.5, 0.75}, 0.05);
        df.stat().crosstab("department", "age").show();
        df.stat().freqItems(new String[]{"department"}).show();

        df.foreach(row -> System.out.println(row.getString(0)));
        df.foreachPartition(iterator -> {
            while (iterator.hasNext()) {
                System.out.println(iterator.next());
            }
        });

        JavaRDD<Row> rdd = df.toJavaRDD();
        rdd.map(row -> new Tuple2<>(row.getString(0), row.getDouble(3) * 1.1)).take(5);
        rdd.filter(row -> row.getInt(1) > 28).collect();
        rdd.reduce((a, b) -> a.getDouble(3) > b.getDouble(3) ? a : b);
        rdd.groupBy(row -> row.getString(2)).mapValues(list -> list.size()).collect();

        JavaRDD<Integer> intRdd = spark.sparkContext().parallelize(Arrays.asList(1, 2, 3, 4, 5)).toJavaRDD();
        intRdd.map(x -> x * 2).collect();
        intRdd.filter(x -> x % 2 == 0).collect();
        intRdd.flatMap(x -> Arrays.asList(x, x * 2).iterator()).collect();
        intRdd.reduce((a, b) -> a + b);
        intRdd.count();
        intRdd.take(3);
        intRdd.first();
        intRdd.collect();
        intRdd.distinct().collect();
        intRdd.groupBy(x -> x % 2).mapValues(list -> list).collect();
        intRdd.aggregate(0, (acc, x) -> acc + x, (acc1, acc2) -> acc1 + acc2);
        intRdd.fold(0, (acc, x) -> acc + x);

        JavaPairRDD<String, Integer> pairRdd = spark.sparkContext()
                .parallelize(Arrays.asList(new Tuple2<>("a", 1), new Tuple2<>("b", 2), new Tuple2<>("a", 3)))
                .toJavaRDD()
                .mapToPair(tuple -> new Tuple2<>(tuple._1, tuple._2));

        pairRdd.reduceByKey((a, b) -> a + b).collect();
        pairRdd.groupByKey().mapValues(list -> new ArrayList<>(list)).collect();
        pairRdd.sortByKey().collect();
        pairRdd.mapValues(x -> x * 2).collect();
        pairRdd.flatMapValues(x -> Arrays.asList(x, x * 2)).collect();
        pairRdd.keys().collect();
        pairRdd.values().collect();
        pairRdd.join(pairRdd).collect();
        pairRdd.leftOuterJoin(pairRdd).collect();
        pairRdd.rightOuterJoin(pairRdd).collect();
        pairRdd.cogroup(pairRdd).mapValues(tuple -> new Tuple2<>(new ArrayList<>(tuple._1), new ArrayList<>(tuple._2))).collect();

        intRdd.persist(StorageLevel.MEMORY_ONLY());
        intRdd.unpersist();
        intRdd.cache();
        intRdd.unpersist();

        intRdd.repartition(4);
        intRdd.coalesce(2);

        intRdd.saveAsTextFile("rdd_output.txt");

        List<Row> mlData = Arrays.asList(
                RowFactory.create(1, "Hello World", 1.0),
                RowFactory.create(2, "PySpark is great", 0.0),
                RowFactory.create(3, "Machine Learning", 1.0)
        );
        StructType mlSchema = new StructType(new StructField[]{
                new StructField("id", DataTypes.IntegerType, false, Metadata.empty()),
                new StructField("text", DataTypes.StringType, false, Metadata.empty()),
                new StructField("label", DataTypes.DoubleType, false, Metadata.empty())
        });
        Dataset<Row> mlDf = spark.createDataFrame(mlData, mlSchema);

        Tokenizer tokenizer = new Tokenizer().setInputCol("text").setOutputCol("words");
        Dataset<Row> wordsData = tokenizer.transform(mlDf);
        wordsData.show();

        StopWordsRemover remover = new StopWordsRemover().setInputCol("words").setOutputCol("filtered_words");
        Dataset<Row> filteredData = remover.transform(wordsData);
        filteredData.show();

        CountVectorizer cv = new CountVectorizer().setInputCol("filtered_words").setOutputCol("raw_features");
        CountVectorizerModel cvModel = cv.fit(filteredData);
        Dataset<Row> cvData = cvModel.transform(filteredData);
        cvData.show();

        IDF idf = new IDF().setInputCol("raw_features").setOutputCol("features");
        IDFModel idfModel = idf.fit(cvData);
        Dataset<Row> idfData = idfModel.transform(cvData);
        idfData.show();

        StringIndexer stringIndexer = new StringIndexer().setInputCol("text").setOutputCol("text_index");
        StringIndexerModel stringIndexerModel = stringIndexer.fit(mlDf);
        Dataset<Row> indexedData = stringIndexerModel.transform(mlDf);
        indexedData.show();

        OneHotEncoder oneHotEncoder = new OneHotEncoder().setInputCol("text_index").setOutputCol("text_vec");
        OneHotEncoderModel oneHotModel = oneHotEncoder.fit(indexedData);
        Dataset<Row> oneHotData = oneHotModel.transform(indexedData);
        oneHotData.show();

        VectorAssembler assembler = new VectorAssembler().setInputCols(new String[]{"text_index"}).setOutputCol("features");
        Dataset<Row> assembledData = assembler.transform(indexedData);
        assembledData.show();

        StandardScaler scaler = new StandardScaler().setInputCol("features").setOutputCol("scaled_features");
        StandardScalerModel scalerModel = scaler.fit(assembledData);
        Dataset<Row> scaledData = scalerModel.transform(assembledData);
        scaledData.show();

        MinMaxScaler minMaxScaler = new MinMaxScaler().setInputCol("features").setOutputCol("minmax_features");
        MinMaxScalerModel minMaxModel = minMaxScaler.fit(assembledData);
        Dataset<Row> minMaxData = minMaxModel.transform(assembledData);
        minMaxData.show();

        PCA pca = new PCA().setK(2).setInputCol("features").setOutputCol("pca_features");
        PCAModel pcaModel = pca.fit(assembledData);
        Dataset<Row> pcaData = pcaModel.transform(assembledData);
        pcaData.show();

        double[] splits = new double[]{0, 0.5, 1.0};
        Bucketizer bucketizer = new Bucketizer().setSplits(splits).setInputCol("label").setOutputCol("bucketed");
        Dataset<Row> bucketedData = bucketizer.transform(mlDf);
        bucketedData.show();

        QuantileDiscretizer discretizer = new QuantileDiscretizer().setNumBuckets(2).setInputCol("label").setOutputCol("discretized");
        QuantileDiscretizerModel discretizerModel = discretizer.fit(mlDf);
        Dataset<Row> discretizedData = discretizerModel.transform(mlDf);
        discretizedData.show();

        Binarizer binarizer = new Binarizer().setThreshold(0.5).setInputCol("label").setOutputCol("binarized");
        Dataset<Row> binarizedData = binarizer.transform(mlDf);
        binarizedData.show();

        NGram ngram = new NGram().setN(2).setInputCol("words").setOutputCol("ngrams");
        Dataset<Row> ngramData = ngram.transform(wordsData);
        ngramData.show();

        ChiSqSelector chiSqSelector = new ChiSqSelector().setFeaturesCol("features").setLabelCol("label").setOutputCol("selected_features");
        ChiSqSelectorModel chiSqModel = chiSqSelector.fit(idfData);
        Dataset<Row> chiSqData = chiSqModel.transform(idfData);
        chiSqData.show();

        Normalizer normalizer = new Normalizer().setInputCol("features").setOutputCol("norm_features");
        Dataset<Row> normalizedData = normalizer.transform(idfData);
        normalizedData.show();

        Dataset<Row>[] trainingData = mlDf.randomSplit(new double[]{0.8, 0.2});

        LogisticRegression lr = new LogisticRegression().setFeaturesCol("features").setLabelCol("label");
        LogisticRegressionModel lrModel = lr.fit(assembledData);
        Dataset<Row> lrPredictions = lrModel.transform(assembledData);
        lrPredictions.show();

        DecisionTreeClassifier dt = new DecisionTreeClassifier().setFeaturesCol("features").setLabelCol("label");
        DecisionTreeClassificationModel dtModel = dt.fit(assembledData);
        Dataset<Row> dtPredictions = dtModel.transform(assembledData);
        dtPredictions.show();

        RandomForestClassifier rf = new RandomForestClassifier().setFeaturesCol("features").setLabelCol("label");
        RandomForestClassificationModel rfModel = rf.fit(assembledData);
        Dataset<Row> rfPredictions = rfModel.transform(assembledData);
        rfPredictions.show();

        GBTClassifier gbt = new GBTClassifier().setFeaturesCol("features").setLabelCol("label");
        GBTClassificationModel gbtModel = gbt.fit(assembledData);
        Dataset<Row> gbtPredictions = gbtModel.transform(assembledData);
        gbtPredictions.show();

        NaiveBayes nb = new NaiveBayes().setFeaturesCol("features").setLabelCol("label");
        NaiveBayesModel nbModel = nb.fit(assembledData);
        Dataset<Row> nbPredictions = nbModel.transform(assembledData);
        nbPredictions.show();

        List<Row> regData = Arrays.asList(
                RowFactory.create(1, 25, 75000.0),
                RowFactory.create(2, 30, 90000.0),
                RowFactory.create(3, 35, 120000.0),
                RowFactory.create(4, 28, 80000.0)
        );
        StructType regSchema = new StructType(new StructField[]{
                new StructField("id", DataTypes.IntegerType, false, Metadata.empty()),
                new StructField("age", DataTypes.IntegerType, false, Metadata.empty()),
                new StructField("salary", DataTypes.DoubleType, false, Metadata.empty())
        });
        Dataset<Row> regDf = spark.createDataFrame(regData, regSchema);

        VectorAssembler regAssembler = new VectorAssembler().setInputCols(new String[]{"age"}).setOutputCol("features");
        Dataset<Row> regAssembled = regAssembler.transform(regDf);

        LinearRegression linearReg = new LinearRegression().setFeaturesCol("features").setLabelCol("salary");
        LinearRegressionModel linearRegModel = linearReg.fit(regAssembled);
        Dataset<Row> linearRegPredictions = linearRegModel.transform(regAssembled);
        linearRegPredictions.show();

        DecisionTreeRegressor dtReg = new DecisionTreeRegressor().setFeaturesCol("features").setLabelCol("salary");
        DecisionTreeRegressionModel dtRegModel = dtReg.fit(regAssembled);
        Dataset<Row> dtRegPredictions = dtRegModel.transform(regAssembled);
        dtRegPredictions.show();

        RandomForestRegressor rfReg = new RandomForestRegressor().setFeaturesCol("features").setLabelCol("salary");
        RandomForestRegressionModel rfRegModel = rfReg.fit(regAssembled);
        Dataset<Row> rfRegPredictions = rfRegModel.transform(regAssembled);
        rfRegPredictions.show();

        GBTRegressor gbtReg = new GBTRegressor().setFeaturesCol("features").setLabelCol("salary");
        GBTRegressionModel gbtRegModel = gbtReg.fit(regAssembled);
        Dataset<Row> gbtRegPredictions = gbtRegModel.transform(regAssembled);
        gbtRegPredictions.show();

        List<Row> clusterData = Arrays.asList(
                RowFactory.create(1, Vectors.dense(1.0, 2.0)),
                RowFactory.create(2, Vectors.dense(2.0, 3.0)),
                RowFactory.create(3, Vectors.dense(3.0, 4.0)),
                RowFactory.create(4, Vectors.dense(4.0, 5.0))
        );
        StructType clusterSchema = new StructType(new StructField[]{
                new StructField("id", DataTypes.IntegerType, false, Metadata.empty()),
                new StructField("features", new VectorUDT(), false, Metadata.empty())
        });
        Dataset<Row> clusterDf = spark.createDataFrame(clusterData, clusterSchema);

        KMeans kmeans = new KMeans().setK(2).setFeaturesCol("features");
        KMeansModel kmeansModel = kmeans.fit(clusterDf);
        Dataset<Row> kmeansPredictions = kmeansModel.transform(clusterDf);
        kmeansPredictions.show();

        BisectingKMeans bisectingKmeans = new BisectingKMeans().setK(2).setFeaturesCol("features");
        BisectingKMeansModel bisectingModel = bisectingKmeans.fit(clusterDf);
        Dataset<Row> bisectingPredictions = bisectingModel.transform(clusterDf);
        bisectingPredictions.show();

        GaussianMixture gmm = new GaussianMixture().setK(2).setFeaturesCol("features");
        GaussianMixtureModel gmmModel = gmm.fit(clusterDf);
        Dataset<Row> gmmPredictions = gmmModel.transform(clusterDf);
        gmmPredictions.show();

        BinaryClassificationEvaluator binaryEvaluator = new BinaryClassificationEvaluator()
                .setLabelCol("label")
                .setMetricName("areaUnderROC");
        double auc = binaryEvaluator.evaluate(lrPredictions);
        System.out.println("AUC: " + auc);

        MulticlassClassificationEvaluator multiEvaluator = new MulticlassClassificationEvaluator()
                .setLabelCol("label")
                .setMetricName("accuracy");
        double accuracy = multiEvaluator.evaluate(lrPredictions);
        System.out.println("Accuracy: " + accuracy);

        RegressionEvaluator regEvaluator = new RegressionEvaluator()
                .setLabelCol("salary")
                .setMetricName("rmse");
        double rmse = regEvaluator.evaluate(linearRegPredictions);
        System.out.println("RMSE: " + rmse);

        ClusteringEvaluator clusterEvaluator = new ClusteringEvaluator().setFeaturesCol("features");
        double silhouette = clusterEvaluator.evaluate(kmeansPredictions);
        System.out.println("Silhouette: " + silhouette);

        List<Row> alsData = Arrays.asList(
                RowFactory.create(1, 1, 5.0),
                RowFactory.create(1, 2, 3.0),
                RowFactory.create(2, 1, 4.0),
                RowFactory.create(2, 2, 2.0)
        );
        StructType alsSchema = new StructType(new StructField[]{
                new StructField("user", DataTypes.IntegerType, false, Metadata.empty()),
                new StructField("item", DataTypes.IntegerType, false, Metadata.empty()),
                new StructField("rating", DataTypes.DoubleType, false, Metadata.empty())
        });
        Dataset<Row> alsDf = spark.createDataFrame(alsData, alsSchema);

        ALS als = new ALS().setUserCol("user").setItemCol("item").setRatingCol("rating");
        ALSModel alsModel = als.fit(alsDf);
        Dataset<Row> alsPredictions = alsModel.transform(alsDf);
        alsPredictions.show();

        Pipeline pipeline = new Pipeline().setStages(new PipelineStage[]{tokenizer, remover, cv, idf});
        PipelineModel pipelineModel = pipeline.fit(mlDf);
        Dataset<Row> pipelineData = pipelineModel.transform(mlDf);
        pipelineData.show();

        List<Row> corData = Arrays.asList(
                RowFactory.create(1.0, 2.0),
                RowFactory.create(2.0, 3.0),
                RowFactory.create(3.0, 4.0)
        );
        StructType corSchema = new StructType(new StructField[]{
                new StructField("feature1", DataTypes.DoubleType, false, Metadata.empty()),
                new StructField("feature2", DataTypes.DoubleType, false, Metadata.empty())
        });
        Dataset<Row> corDf = spark.createDataFrame(corData, corSchema);

        VectorAssembler corAssembler = new VectorAssembler().setInputCols(new String[]{"feature1", "feature2"}).setOutputCol("features");
        Dataset<Row> corAssembled = corAssembler.transform(corDf);

        Row correlationMatrix = Correlation.corr(corAssembled, "features").head();
        System.out.println("Correlation Matrix: " + correlationMatrix);

        spark.stop();
    }
}
