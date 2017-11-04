
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
scala> mytable1.show()
+---+---------+-------------+-------+---+---+-------------+----+
| iD|     name|Triglycerides|Glucose|LDL|HDL|T_Cholesterol|Risk|
+---+---------+-------------+-------+---+---+-------------+----+
|  1| Person#1|          150|     92|192| 55|          176|  No|
|  2| Person#2|          214|     93|193| 29|          179| Yes|
|  3| Person#3|          130|     98|198| 55|          168|  No|
|  4| Person#4|          199|     96|196| 48|          169| Yes|
|  5| Person#5|          216|    100|200| 29|          200| Yes|
|  6| Person#6|          170|     95|195| 31|          183|  No|
|  7| Person#7|          200|    113|213| 22|          246| Yes|
|  8| Person#8|          212|    103|203| 29|          209| Yes|
|  9| Person#9|          215|     97|197| 55|          165| Yes|
| 10|Person#10|          120|    102|202| 75|          206|  No|
| 11|Person#11|          170|     96|196| 29|          188| Yes|
| 12|Person#12|          230|    130|230| 45|          274| Yes|
| 13|Person#13|          200|    115|215| 29|          245| Yes|
| 14|Person#14|          170|     96|196| 57|          188| Yes|
| 15|Person#15|          190|     99|199| 44|          182| Yes|
| 16|Person#16|          213|    150|250| 32|          347| Yes|
| 17|Person#17|          233|    100|200| 43|          245| Yes|
| 18|Person#18|           99|    103|203| 29|          209|  No|
| 19|Person#19|          200|     93|193| 33|          175| Yes|
| 20|Person#20|          199|     95|195| 21|          193| Yes|
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
|  1| Person#1|          150|     92|192| 55|          176|  No|
|  2| Person#2|          214|     93|193| 29|          179| Yes|
|  3| Person#3|          130|     98|198| 55|          168|  No|
|  4| Person#4|          199|     96|196| 48|          169| Yes|
|  5| Person#5|          216|    100|200| 29|          200| Yes|
|  6| Person#6|          170|     95|195| 31|          183|  No|
|  7| Person#7|          200|    113|213| 22|          246| Yes|
|  8| Person#8|          212|    103|203| 29|          209| Yes|
|  9| Person#9|          215|     97|197| 55|          165| Yes|
| 10|Person#10|          120|    102|202| 75|          206|  No|
| 11|Person#11|          170|     96|196| 29|          188| Yes|
| 12|Person#12|          230|    130|230| 45|          274| Yes|
| 13|Person#13|          200|    115|215| 29|          245| Yes|
| 14|Person#14|          170|     96|196| 57|          188| Yes|
| 15|Person#15|          190|     99|199| 44|          182| Yes|
| 16|Person#16|          213|    150|250| 32|          347| Yes|
| 17|Person#17|          233|    100|200| 43|          245| Yes|
| 18|Person#18|           99|    103|203| 29|          209|  No|
| 19|Person#19|          200|     93|193| 33|          175| Yes|
| 20|Person#20|          199|     95|195| 21|          193| Yes|
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
|  1| Person#1|          150|     92|192| 55|          176|  No|
|  2| Person#2|          214|     93|193| 29|          179| Yes|
|  3| Person#3|          130|     98|198| 55|          168|  No|
|  4| Person#4|          199|     96|196| 48|          169| Yes|
|  5| Person#5|          216|    100|200| 29|          200| Yes|
|  6| Person#6|          170|     95|195| 31|          183|  No|
|  7| Person#7|          200|    113|213| 22|          246| Yes|
|  8| Person#8|          212|    103|203| 29|          209| Yes|
|  9| Person#9|          215|     97|197| 55|          165| Yes|
| 10|Person#10|          120|    102|202| 75|          206|  No|
| 11|Person#11|          170|     96|196| 29|          188| Yes|
| 12|Person#12|          230|    130|230| 45|          274| Yes|
| 13|Person#13|          200|    115|215| 29|          245| Yes|
| 14|Person#14|          170|     96|196| 57|          188| Yes|
| 15|Person#15|          190|     99|199| 44|          182| Yes|
| 16|Person#16|          213|    150|250| 32|          347| Yes|
| 17|Person#17|          233|    100|200| 43|          245| Yes|
| 18|Person#18|           99|    103|203| 29|          209|  No|
| 19|Person#19|          200|     93|193| 33|          175| Yes|
| 20|Person#20|          199|     95|195| 21|          193| Yes|
+---+---------+-------------+-------+---+---+-------------+----+

// If you want you can save it as parquet file.
scala> mytable3.write.format("parquet").save("Your_directory/table3.parquet")





