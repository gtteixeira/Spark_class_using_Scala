
// The best way to learn Spark is through examples
// In this file we present cases using Spark 2.2.0 with Scala 2.11.8
// We will show examples of analysis using Spark directly in the REPL with SCALA language.
//NOTE: The examples used here are fictional, created by myself.
// <Portugues>  A melhor maneira de aprender Spark é através de exemplos
// Neste arquivo apresentamos casos usando o Spark 2.2.0 com Scala 2.11.8
// Nos mostraremos exemplos de analises usando o Spark diretamente no REPL com a linguagem SCALA.
//OBS: Os exemplos usados aqui são fictícios, criados por mim.


// Inicializing Spark.
$ spark-shell
 
//Creating our first Dataframe. Loading a csv file. You can use any kind of file: txt, csv, parquet, ...  
//We choose the name table1DF for our Dataframe/Dataset, but you can put any name you want.
scala> val table1DF = spark.read.format( "csv" ).load("Your_directory/pacients.csv")
table1DF: org.apache.spark.sql.DataFrame = [_c0: string, _c1: string ... 6 more fields]//Remmember, this is the return of the REPL.

//Renaming columns. The order is very important!!
scala> val mytable1 = table1DF.toDF ( "iD", "name", "Triglycerides", "Glucose", "LDL", "HDL", "T_Cholesterol", "Risk")
mytable1: org.apache.spark.sql.DataFrame = [iD: string, name: string ... 6 more fields]

//Now we ask the REPL to show our table imported from the csv file, and with new names in the columns.
//We can see our (fictional) table is about blood analysis results, and some health risk related to these levels.
scala> mytable1.show()
+---+---------+-------------+-------+---+---+-------------+----+
| iD|     name|Triglycerides|Glucose|LDL|HDL|T_Cholesterol|Risk|
+---+---------+-------------+-------+---+---+-------------+----+
|  1| Person#1|          150|     92|192| 55|          247|  No|
|  2| Person#2|          214|     93|193| 29|          222| Yes|
|  3| Person#3|          130|     98|198| 55|          253|  No|
|  4| Person#4|          199|     96|196| 48|          244| Yes|
|  5| Person#5|          216|    100|200| 29|          229| Yes|
|  6| Person#6|          170|     95|195| 31|          226|  No|
|  7| Person#7|          200|    113|213| 22|          235| Yes|
|  8| Person#8|          212|    103|203| 29|          232| Yes|
|  9| Person#9|          215|     97|197| 55|          252| Yes|
| 10|Person#10|          120|    102|202| 75|          277|  No|
| 11|Person#11|          170|     96|196| 29|          225| Yes|
| 12|Person#12|          230|    130|230| 45|          275| Yes|
| 13|Person#13|          200|    115|215| 29|          244| Yes|
| 14|Person#14|          170|     96|196| 57|          253| Yes|
| 15|Person#15|          190|     99|199| 44|          243| Yes|
| 16|Person#16|          213|    150|250| 32|          282| Yes|
| 17|Person#17|          233|    100|200| 43|          243| Yes|
| 18|Person#18|           99|    103|203| 29|          232|  No|
| 19|Person#19|          200|     93|193| 33|          226| Yes|
| 20|Person#20|          199|     95|195| 21|          216| Yes|
+---+---------+-------------+-------+---+---+-------------+----+

//If you want to see the Schema of the table, use the name of your Dataframe plus the command printSchema().
scala> mytable1.printSchema()
root
 |-- iD: string (nullable = true)
 |-- name: string (nullable = true)
 |-- Triglycerides: string (nullable = true)
 |-- Glucose: string (nullable = true)
 |-- LDL: string (nullable = true)
 |-- HDL: string (nullable = true)
 |-- T_Cholesterol: string (nullable = true)
 |-- Risk: string (nullable = true)


// In order to use SQL we create a temporary view.
scala> mytable1.createOrReplaceTempView("mytable1")

//Now we can use SQL to do many things easily. See some examples:
scala> spark.sql("SELECT * FROM mytable1").show
+---+---------+-------------+-------+---+---+-------------+----+
| iD|     name|Triglycerides|Glucose|LDL|HDL|T_Cholesterol|Risk|
+---+---------+-------------+-------+---+---+-------------+----+
|  1| Person#1|          150|     92|192| 55|          247|  No|
|  2| Person#2|          214|     93|193| 29|          222| Yes|
|  3| Person#3|          130|     98|198| 55|          253|  No|
|  4| Person#4|          199|     96|196| 48|          244| Yes|
|  5| Person#5|          216|    100|200| 29|          229| Yes|
|  6| Person#6|          170|     95|195| 31|          226|  No|
|  7| Person#7|          200|    113|213| 22|          235| Yes|
|  8| Person#8|          212|    103|203| 29|          232| Yes|
|  9| Person#9|          215|     97|197| 55|          252| Yes|
| 10|Person#10|          120|    102|202| 75|          277|  No|
| 11|Person#11|          170|     96|196| 29|          225| Yes|
| 12|Person#12|          230|    130|230| 45|          275| Yes|
| 13|Person#13|          200|    115|215| 29|          244| Yes|
| 14|Person#14|          170|     96|196| 57|          253| Yes|
| 15|Person#15|          190|     99|199| 44|          243| Yes|
| 16|Person#16|          213|    150|250| 32|          282| Yes|
| 17|Person#17|          233|    100|200| 43|          243| Yes|
| 18|Person#18|           99|    103|203| 29|          232|  No|
| 19|Person#19|          200|     93|193| 33|          226| Yes|
| 20|Person#20|          199|     95|195| 21|          216| Yes|
+---+---------+-------------+-------+---+---+-------------+----+

scala> spark.sql("SELECT name, LDL, HDL FROM mytable1 WHERE LDL > 100").show
+---------+---+---+
|     name|LDL|HDL|
+---------+---+---+
| Person#1|192| 55|
| Person#2|193| 29|
| Person#3|198| 55|
| Person#4|196| 48|
| Person#5|200| 29|
| Person#6|195| 31|
| Person#7|213| 22|
| Person#8|203| 29|
| Person#9|197| 55|
|Person#10|202| 75|
|Person#11|196| 29|
|Person#12|230| 45|
|Person#13|215| 29|
|Person#14|196| 57|
|Person#15|199| 44|
|Person#16|250| 32|
|Person#17|200| 43|
|Person#18|203| 29|
|Person#19|193| 33|
|Person#20|195| 21|
+---------+---+---+


scala> spark.sql("SELECT MAX(LDL) FROM mytable1").show
+--------+
|max(LDL)|
+--------+
|     250|
+--------+

// Once Spark creates a Hive table under the rood, you can save your table as an ORC file.
// For example, we create a new table "mytable3".
scala> val mytable3 = spark.sql("SELECT * FROM mytable1")
mytable3: org.apache.spark.sql.DataFrame = [iD: string, name: string ... 6 more fields]

//And save here as an ORC file.
scala> mytable3.write.format("orc").save("Your_directory/table3_orc")

// Now we can open our ORC file.
scala> spark.read.format( "orc" ).load("Your_directory/table3_orc").show
+---+---------+-------------+-------+---+---+-------------+----+
| iD|     name|Triglycerides|Glucose|LDL|HDL|T_Cholesterol|Risk|
+---+---------+-------------+-------+---+---+-------------+----+
|  1| Person#1|          150|     92|192| 55|          247|  No|
|  2| Person#2|          214|     93|193| 29|          222| Yes|
|  3| Person#3|          130|     98|198| 55|          253|  No|
|  4| Person#4|          199|     96|196| 48|          244| Yes|
|  5| Person#5|          216|    100|200| 29|          229| Yes|
|  6| Person#6|          170|     95|195| 31|          226|  No|
|  7| Person#7|          200|    113|213| 22|          235| Yes|
|  8| Person#8|          212|    103|203| 29|          232| Yes|
|  9| Person#9|          215|     97|197| 55|          252| Yes|
| 10|Person#10|          120|    102|202| 75|          277|  No|
| 11|Person#11|          170|     96|196| 29|          225| Yes|
| 12|Person#12|          230|    130|230| 45|          275| Yes|
| 13|Person#13|          200|    115|215| 29|          244| Yes|
| 14|Person#14|          170|     96|196| 57|          253| Yes|
| 15|Person#15|          190|     99|199| 44|          243| Yes|
| 16|Person#16|          213|    150|250| 32|          282| Yes|
| 17|Person#17|          233|    100|200| 43|          243| Yes|
| 18|Person#18|           99|    103|203| 29|          232|  No|
| 19|Person#19|          200|     93|193| 33|          226| Yes|
| 20|Person#20|          199|     95|195| 21|          216| Yes|
+---+---------+-------------+-------+---+---+-------------+----+


// If you want you can save it as parquet file.
scala> mytable3.write.format("parquet").save("Your_directory/table3.parquet")


///////////////////////////////////////////Part #2 - MACHINE LEARNING ////////////////////////////////////

// Now we will submerge in the Machile Learning world using SPARK!!


//Firstly, we load a table from JSON file, and we named that as "mytable1".
scala> val mytable1 = spark.read.format( "json" ).load("Your_directory/pacients2ml.json")
mytable1: org.apache.spark.sql.DataFrame = [Glucose: bigint, HDL: bigint ... 6 more fields]

//Ask the REPL to show the table. It is always a good idea!!
scala> mytable1.show()
+-------+---+---+----+-------------+-------------+---+---------+
|Glucose|HDL|LDL|Risk|T_Cholesterol|Triglycerides| iD|     name|
+-------+---+---+----+-------------+-------------+---+---------+
|     92| 55|192|   0|          247|          150|  1| Person#1|
|     93| 29|193|   1|          222|          214|  2| Person#2|
|     98| 55|198|   0|          253|          130|  3| Person#3|
|     96| 48|196|   1|          244|          199|  4| Person#4|
|    100| 29|200|   1|          229|          216|  5| Person#5|
|     95| 31|195|   0|          226|          170|  6| Person#6|
|    113| 22|213|   1|          235|          200|  7| Person#7|
|    103| 29|203|   1|          232|          212|  8| Person#8|
|     97| 55|197|   1|          252|          215|  9| Person#9|
|    102| 75|202|   0|          277|          120| 10|Person#10|
|     96| 29|196|   1|          225|          170| 11|Person#11|
|    130| 45|230|   1|          275|          230| 12|Person#12|
|    115| 29|215|   1|          255|          200| 13|Person#13|
|     96| 57|196|   1|          253|          170| 14|Person#14|
|     99| 44|199|   1|          243|          190| 15|Person#15|
|    150| 32|250|   1|          282|          213| 16|Person#16|
|    100| 43|200|   1|          243|          233| 17|Person#17|
|    103| 29|203|   0|          232|           99| 18|Person#18|
|     93| 33|193|   1|          226|          200| 19|Person#19|
|     95| 21|195|   1|          216|          199| 20|Person#20|
+-------+---+---+----+-------------+-------------+---+---------+


//...and ask for the Schema, as well.
scala> mytable1.printSchema()
root
 |-- Glucose: long (nullable = true)
 |-- HDL: long (nullable = true)
 |-- LDL: long (nullable = true)
 |-- Risk: long (nullable = true)
 |-- T_Cholesterol: long (nullable = true)
 |-- Triglycerides: long (nullable = true)
 |-- iD: long (nullable = true)
 |-- name: string (nullable = true)


//Before we run some Machine Learning algorithms, It is necessary to treat our data and to create features column.  
//we need to import some libraries.
scala> import org.apache.spark.ml.feature.Interaction
import org.apache.spark.ml.feature.Interaction

scala> import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.feature.VectorAssembler

// Secondly, we create a vector of features. To do that, we use a transformation called "VectorAssembler", to combine a list of features columns into a single vector column.
// Our selected features, or independent variables, in this examples will be: "Triglycerides", "Glucose", "LDL", "HDL", "T_Cholesterol". We use these features to predict the Risk, our target.
scala> val assembler = new VectorAssembler().setInputCols(Array( "Triglycerides", "Glucose", "LDL", "HDL", "T_Cholesterol")).setOutputCol("features")
assembler: org.apache.spark.ml.feature.VectorAssembler = vecAssembler_07a7dd9c9103

//Calling the assembler to mytable1.
scala> val data2 = assembler.transform(mytable1)
data2: org.apache.spark.sql.DataFrame = [Glucose: bigint, HDL: bigint ... 7 more fields]

 
// Asking the REPL to show our new table, with the new features column attached.
scala> data2.show()
+-------+---+---+----+-------------+-------------+---+---------+--------------------+
|Glucose|HDL|LDL|Risk|T_Cholesterol|Triglycerides| iD|     name|            features|
+-------+---+---+----+-------------+-------------+---+---------+--------------------+
|     92| 55|192|   0|          247|          150|  1| Person#1|[150.0,92.0,192.0...|
|     93| 29|193|   1|          222|          214|  2| Person#2|[214.0,93.0,193.0...|
|     98| 55|198|   0|          253|          130|  3| Person#3|[130.0,98.0,198.0...|
|     96| 48|196|   1|          244|          199|  4| Person#4|[199.0,96.0,196.0...|
|    100| 29|200|   1|          229|          216|  5| Person#5|[216.0,100.0,200....|
|     95| 31|195|   0|          226|          170|  6| Person#6|[170.0,95.0,195.0...|
|    113| 22|213|   1|          235|          200|  7| Person#7|[200.0,113.0,213....|
|    103| 29|203|   1|          232|          212|  8| Person#8|[212.0,103.0,203....|
|     97| 55|197|   1|          252|          215|  9| Person#9|[215.0,97.0,197.0...|
|    102| 75|202|   0|          277|          120| 10|Person#10|[120.0,102.0,202....|
|     96| 29|196|   1|          225|          170| 11|Person#11|[170.0,96.0,196.0...|
|    130| 45|230|   1|          275|          230| 12|Person#12|[230.0,130.0,230....|
|    115| 29|215|   1|          255|          200| 13|Person#13|[200.0,115.0,215....|
|     96| 57|196|   1|          253|          170| 14|Person#14|[170.0,96.0,196.0...|
|     99| 44|199|   1|          243|          190| 15|Person#15|[190.0,99.0,199.0...|
|    150| 32|250|   1|          282|          213| 16|Person#16|[213.0,150.0,250....|
|    100| 43|200|   1|          243|          233| 17|Person#17|[233.0,100.0,200....|
|    103| 29|203|   0|          232|           99| 18|Person#18|[99.0,103.0,203.0...|
|     93| 33|193|   1|          226|          200| 19|Person#19|[200.0,93.0,193.0...|
|     95| 21|195|   1|          216|          199| 20|Person#20|[199.0,95.0,195.0...|
+-------+---+---+----+-------------+-------------+---+---------+--------------------+

//... and the Schema of the table, as well.
scala> data2.printSchema()
root
 |-- Glucose: long (nullable = true)
 |-- HDL: long (nullable = true)
 |-- LDL: long (nullable = true)
 |-- Risk: long (nullable = true)
 |-- T_Cholesterol: long (nullable = true)
 |-- Triglycerides: long (nullable = true)
 |-- iD: long (nullable = true)
 |-- name: string (nullable = true)
 |-- features: vector (nullable = true)




/////////////////////////  Using the Decision tree classifier   /////////////////////////


// After these procedures, we call our Machine Learning method. In this example we use the "Decision Tree Classifier".
// Decision tree is an important method of Machine Learning, because it is very versatile and robust as well. However same times it can suffer overfitting.
// Firtly, we need to import the Decision Tree libraries.
scala> import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.Pipeline

scala> import org.apache.spark.ml.classification.DecisionTreeClassificationModel 
import org.apache.spark.ml.classification.DecisionTreeClassificationModel

scala> import org.apache.spark.ml.classification.DecisionTreeClassifier
import org.apache.spark.ml.classification.DecisionTreeClassifier

scala> import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator

scala> import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorIndexer}
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorIndexer}


// Now, we call a indexer method to set our Risk column as our Indexed Label column. Fit on whole dataset ("data2") to include all labels in index.
scala> val labelIndexer = new StringIndexer().setInputCol("Risk").setOutputCol("indexedLabel").fit(data2)
labelIndexer: org.apache.spark.ml.feature.StringIndexerModel = strIdx_7362c2e3c222

//By the same way, we set the features column as our indexed Features column.
// Set maxCategories of features. In this case we use 5. In Spark, with > 4 distinct values are treated as continuous.
scala> val featureIndexer = new VectorIndexer().setInputCol("features").setOutputCol("indexedFeatures").setMaxCategories(5).fit(data2)
featureIndexer: org.apache.spark.ml.feature.VectorIndexerModel = vecIdx_1a1f9aa3cb54

//Here, we contruct our Decision Tree Classifier, with our indexed columns, Label and Features.
scala> val dt = new DecisionTreeClassifier().setLabelCol("indexedLabel").setFeaturesCol("indexedFeatures")
dt: org.apache.spark.ml.classification.DecisionTreeClassifier = dtc_65b5798ebde5

//...and use a label converter method to convert indexed labels back to original labels.
scala> val labelConverter = new IndexToString().setInputCol("prediction").setOutputCol("predictedLabel").setLabels(labelIndexer.labels)
labelConverter: org.apache.spark.ml.feature.IndexToString = idxToStr_ce05dfe09878

// Chain all the transformations and methods in a single Pipeline.
scala> val pipeline = new Pipeline().setStages(Array(labelIndexer, featureIndexer, dt, labelConverter))
pipeline: org.apache.spark.ml.Pipeline = pipeline_c8f538bdf6d0

// after, train the model using our Pipeline. It will run all steps automatically.
scala> val model = pipeline.fit(data2)  
model: org.apache.spark.ml.PipelineModel = pipeline_c8f538bdf6d0


// ...and finally make predictions!!
// In this case we use the same dataset as an example. In real situation, of course, you would have a different dataset.
cala> val predictions = model.transform(data2) 
predictions: org.apache.spark.sql.DataFrame = [Glucose: bigint, HDL: bigint ... 13 more fields]

//Select the columns that you want to display, and compare the target column Risk with the predicted by the Decision Tree Method.
// Exactly the same results. It was great!!
//Note: Once we have used the same dataset for training and test, we should be concerned about overfitting. But don't worry about that right now!
scala> predictions.select("name","predictedLabel", "Risk", "features").show()
+---------+--------------+----+--------------------+
|     name|predictedLabel|Risk|            features|
+---------+--------------+----+--------------------+
| Person#1|             0|   0|[150.0,92.0,192.0...|
| Person#2|             1|   1|[214.0,93.0,193.0...|
| Person#3|             0|   0|[130.0,98.0,198.0...|
| Person#4|             1|   1|[199.0,96.0,196.0...|
| Person#5|             1|   1|[216.0,100.0,200....|
| Person#6|             0|   0|[170.0,95.0,195.0...|
| Person#7|             1|   1|[200.0,113.0,213....|
| Person#8|             1|   1|[212.0,103.0,203....|
| Person#9|             1|   1|[215.0,97.0,197.0...|
|Person#10|             0|   0|[120.0,102.0,202....|
|Person#11|             1|   1|[170.0,96.0,196.0...|
|Person#12|             1|   1|[230.0,130.0,230....|
|Person#13|             1|   1|[200.0,115.0,215....|
|Person#14|             1|   1|[170.0,96.0,196.0...|
|Person#15|             1|   1|[190.0,99.0,199.0...|
|Person#16|             1|   1|[213.0,150.0,250....|
|Person#17|             1|   1|[233.0,100.0,200....|
|Person#18|             0|   0|[99.0,103.0,203.0...|
|Person#19|             1|   1|[200.0,93.0,193.0...|
|Person#20|             1|   1|[199.0,95.0,195.0...|
+---------+--------------+----+--------------------+

// If you want, you can select (prediction, true label) and compute test error.
scala> val evaluator = new MulticlassClassificationEvaluator().setLabelCol("indexedLabel").setPredictionCol("prediction").setMetricName("accuracy")
evaluator: org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator = mcEval_014a5d67dbe7

scala> val accuracy = evaluator.evaluate(predictions)
accuracy: Double = 1.0

// This is the error results, so good!
scala> println("Test Error = " + (1.0 - accuracy))
Test Error = 0.0


// One of the greatest properties of decision tree models is that you can see the final tree. If you want to see, first ask the contructor...
scala> val treeModel = model.stages(2).asInstanceOf[DecisionTreeClassificationModel]
treeModel: org.apache.spark.ml.classification.DecisionTreeClassificationModel = DecisionTreeClassificationModel (uid=dtc_65b5798ebde5) of depth 3 with 7 nodes

// ...and print it. Of course it is not so beautiful but you can see that!!
scala> println("Learned classification tree model:\n" + treeModel.toDebugString) 
Learned classification tree model:
DecisionTreeClassificationModel (uid=dtc_65b5798ebde5) of depth 3 with 7 nodes
  If (feature 0 <= 150.0)
   Predict: 1.0
  Else (feature 0 > 150.0)
   If (feature 0 <= 170.0)
    If (feature 1 <= 95.0)
     Predict: 1.0
    Else (feature 1 > 95.0)
     Predict: 0.0
   Else (feature 0 > 170.0)
    Predict: 0.0

//Congratulations! You have finished your Decision Tree Classifier with honor!!







