package com.opensistemas

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.{lit,col}
import org.apache.spark.graphx
import org.graphframes.GraphFrame

object DemoCloudFest {
  def main(args: Array[String]) = {
    val spark = SparkSession.builder.
      master("local")
      .appName("spark session example")
      .getOrCreate()

    import spark.implicits._
    import spark.sqlContext.implicits._

    if (args.length != 1) {
      println("Usage: DemoCloudFest gs|fs")
      sys.exit(-1)
    }

    val relacionesPersonales = StructType(Array(
      StructField("src", IntegerType, false),
      StructField("relationship", StringType, false),
      StructField("dst", IntegerType, false)))

    val dataFiles =
     args(0) match {
      case "gs" => "gs://dgonzalez-spark-jobs/data/"
      case _ => "/Users/dgonzalez/Proyectos/google/spark-google/src/main/resources/data/"
    }

    val personas= spark.read.option("inferSchema", "true").option("header", "true").csv(dataFiles + "personas.csv")
    personas.printSchema()
    personas.show(20)
    val seguros = spark.read.option("inferSchema", "true").option("header", "true").csv(dataFiles + "seguros.csv")
    seguros.printSchema()
    seguros.show(20)
    val partes = spark.read.option("inferSchema", "true").option("header", "true").csv(dataFiles + "partes.csv")
        .withColumn("numParte",col("numParte").cast(StringType))
    partes.printSchema()
    partes.show(20)


    val aristasConocidos = personas.select("dni1","dni2")
      .union(personas.select("dni2","dni1"))
      .toDF("src","dst")
      .withColumn("relationship",lit("conoce"))
      .distinct()

    //Suponemos que no hay personas que no tengan seguro
    val verticesPersonas = seguros.select("dni").distinct().toDF("id")

    val aristasCoches = seguros.toDF("src","dst")
      .withColumn("relationship",lit("asegura"))

    val verticesCoches = seguros.select("matricula")
      .withColumnRenamed("matricula","id")
      .distinct()

    val aristasGolpes = partes.select("matricula1","numParte")
      .union(partes.select("matricula2","numParte"))
      .toDF("src","dst")
      .withColumn("relationship",lit("participa"))

    val verticesPartes = partes.select("numParte")
      .withColumnRenamed("numParte","id")

    val vertices = verticesPersonas.union(verticesCoches).union(verticesPartes)
    val aristas = aristasConocidos.union(aristasCoches).union(aristasGolpes)

    val g: GraphFrame = GraphFrame(vertices,aristas)

    //g.vertices.show()
    //g.edges.show()

    val posibles = g.find("(a)-[r1]->(b);(b)-[r2]->(p);(c)-[r3]->(p);(d)-[r4]->(c);(d)-[r5]->(a)")
        .filter("r1.relationship = 'asegura' and r2.relationship = 'participa' and r3.relationship = 'participa' and r4.relationship = 'asegura' and r5.relationship = 'conoce'")
        .filter("b.id != c.id")
        //.select("a.id","d.id")
        .select("p.id").toDF("numParte")
        .distinct()
        .show()



  }
}