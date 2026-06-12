import org.apache.spark.sql.SparkSession
import org.apache.spark.util.LongAccumulator

object Main {
  def main(args: Array[String]): Unit = {
  val starTime = System.currentTimeMillis() // tiempo total de ejecución

  val spark = SparkSession.builder()
  .appName("RedditNER")
  .master("local[*]")
  .config("spark.ui.enabled", "true")
  .config("spark.ui.port", "4040")
  .getOrCreate()
  val sc = spark.sparkContext
  
  // callando un poco al compilador de spark
  spark.sparkContext.setLogLevel("ERROR")

    // Parse command-line arguments
    val cmdArgs = CommandLineArgs.parse(args) match {
      case Some(parsed) => parsed
      case None => return // scopt prints error messages
    }

    // Load subscriptions
    val subscriptionOpts = FileIO.readSubscriptions(cmdArgs.subscriptionFile)

    // Filter out malformed subscriptions (None values)
    val subscriptions = subscriptionOpts.flatten

    // If no valid subscriptions found, exit with error
    if (subscriptions.isEmpty) {
      println("Error: No valid subscriptions found")
      return
    }

    // añadimos acumuladores para calcular estadisticas
    val feedSuccessAcc = sc.longAccumulator("feedSuccess")     //N° de feeds descargados con exito
    val feedFailedAcc = sc.longAccumulator("feedFailed")       //N° de feeds que fallaron
    val postsSuccessAcc = sc.longAccumulator("postsSuccess")   //N° de posts descargados con exito
    val postsFilteredAcc = sc.longAccumulator("postsFiltered") //N° de posts descartados

    // Create an RDD from the subscriptions list
    val subscriptionsRDD = sc.parallelize(subscriptions)

    // Download feeds using an RDD and produce an RDD[Post]
    val downloadsRDD = subscriptionsRDD.map { subscription =>
      // Attempt to fetch feed
      FileIO.downloadFeed(subscription.url) match {
        case Some(feedJson) => 
          try {
            val posts = JsonParser.parsePosts(feedJson, subscription.name) // parsea el json

            feedSuccessAcc.add(1)             // += 1 feed descargado
            postsSuccessAcc.add(posts.length) // += posts descargados

            val filteredPosts = Analyzer.filterEmptyPosts(posts) // filtrado de posts

            postsFilteredAcc.add(posts.length - filteredPosts.length) //conteo posts descartados

            filteredPosts
          } catch {
            case _: Exception =>
              println(s"Warning: Failed to parse posts from '${subscription.name}' (${subscription.url})")

              feedSuccessAcc.add(1)   // descargado, no valido, +=1
              postsSuccessAcc.add(0)  // no valido, no suma, +=0
              postsFilteredAcc.add(0) // no valido, no filtrado, +=0

              List.empty[Post]
          }
        case None =>
          println(s"Warning: Failed to download from '${subscription.name}' (${subscription.url})")

          feedFailedAcc.add(1) // feed invalido +=1
          
          List.empty[Post]
      }
    }.cache()

    // PROCESAMIENTO DE ESTADISTICAS
    // FlatMap to get all posts across feeds, keeping only posts with non-empty title and selftext
    val postsRDD = downloadsRDD.flatMap(identity).cache()


    // If no posts after filtering non-empty title/selftext, exit early with error
    if (postsRDD.count == 0) {
      println("Error: No valid posts downloaded after filtering")
      downloadsRDD.unpersist()
      postsRDD.unpersist()
      spark.stop()
      return
    }

    // Calculate average characters in filtered posts
    val totalChars = postsRDD.map(post => post.title.length + post.selftext.length).sum.toLong
    val avgChars = if (postsRDD.count > 0) (totalChars / postsRDD.count).toInt else 0

    // Prepare statistics
    val stats = Map(
      "feedsSuccess" -> feedSuccessAcc.value.toInt,
      "feedsFailed" -> feedFailedAcc.value.toInt,
      "postsSuccess" -> postsSuccessAcc.value.toInt,
      "postsFailed" -> 0,
      "postsFiltered" -> postsFilteredAcc.value.toInt,
      "avgChars" -> avgChars
    )

    println(Formatters.formatProcessingStats(stats))
    println()

    // PROCESAR ENTIDADES NOMBRADAS

    // Load dictionaries
    val dictionary = Dictionary.loadAll(cmdArgs.entitiesDir)

    // Broadcast dictionary for use in RDD transformations
    val dictBroadcast = sc.broadcast(dictionary)

    val results = postsRDD
     .flatMap { post =>
      val combinedText = post.title + " " + post.selftext
      Analyzer.detectEntities(combinedText, dictBroadcast.value)
      }
      .map { entity =>
        ((entity.entityType, entity.text), 1)
      }
      .reduceByKey(_ + _)
      .sortBy({ case ((tipo, nombre), count) => (-count, tipo)})
      .collect()

    println(Formatters.formatEntityStats(results.toMap, cmdArgs.topK))
    println()

    // PROCESAR ESTADISTICAS DE TIPOS

    val allEntities = postsRDD
      .flatMap(post => Analyzer.detectEntities(post.title + " " + post.selftext, dictBroadcast.value))
      .collect()
      .toList

    val typeStats = Analyzer.countByType(allEntities)
    
    println(Formatters.formatTypeStats(typeStats))
    println()

    // final de ejecucion y calculo
    val endTime = System.currentTimeMillis()
    val duration = (endTime - starTime) / 1000.0
    println(s"\nTiempo total de ejecición: ${duration} segundos.")
    println("Spark UI disponible en http://localhost:4040")

    //limpieza final de cache 
    postsRDD.unpersist()
    downloadsRDD.unpersist()

 // while (true) {
  //Thread.sleep(1000)
//}
    spark.stop()
  }
}
