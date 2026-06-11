import org.apache.spark.sql.SparkSession

object Main {
  def main(args: Array[String]): Unit = {

  val spark = SparkSession.builder()
  .appName("RedditNER")
  .master("local[*]")
  .getOrCreate()
  val sc = spark.sparkContext
  

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

    // Create an RDD from the subscriptions list
    val subscriptionsRDD = sc.parallelize(subscriptions)

    // Download feeds using an RDD and produce an RDD[Post]
    val downloadsRDD = subscriptionsRDD.map { subscription =>
      // Attempt to fetch feed
      val feedOpt = FileIO.downloadFeed(subscription.url)
      if (feedOpt.isEmpty) {
        println(s"Warning: Failed to download from '${subscription.name}' (${subscription.url})")
        (false, List.empty[Post])
      } else {
        // Attempt to parse posts from feed
        try {
          val posts = JsonParser.parsePosts(feedOpt.get, subscription.name)
          (true, posts)
        } catch {
          case e: Exception =>
            println(s"Warning: Failed to parse posts from '${subscription.name}' (${subscription.url})")
            (false, List.empty[Post])
        }
      }
    }.cache()

    // Count feed successes/failures
    val feedsSuccess = downloadsRDD.filter(_._1).count().toInt
    val feedsFailed = downloadsRDD.count().toInt - feedsSuccess

    // FlatMap to get all posts across feeds, keeping only posts with non-empty title and selftext
    val postsRDD = downloadsRDD.flatMap(_._2)

    // Collect posts for downstream (the rest of the pipeline expects local collections)
    val allPosts = postsRDD.collect().toList


    // If no posts after filtering non-empty title/selftext, exit early with error
    if (allPosts.isEmpty) {
      println("Error: No valid posts downloaded after filtering")
      return
    }

    // Additional filtering step (keeps previous behavior if Analyzer filters further)
    val filteredPosts = Analyzer.filterEmptyPosts(allPosts)
    val postsFiltered = allPosts.length - filteredPosts.length

    val postsSuccess = filteredPosts.length
    val postsFailed = downloadsRDD.filter(_._2.isEmpty).count().toInt

    // Calculate average characters in filtered posts
    val totalChars = filteredPosts.map(post => post.title.length + post.selftext.length).sum
    val avgChars = if (filteredPosts.nonEmpty) totalChars / filteredPosts.length else 0

    // Prepare statistics
    val stats = Map(
      "feedsSuccess" -> feedsSuccess,
      "feedsFailed" -> feedsFailed,
      "postsSuccess" -> postsSuccess,
      "postsFailed" -> postsFailed,
      "postsFiltered" -> postsFiltered,
      "avgChars" -> avgChars
    )

    // Print output
      // Explicitly print requested statistics
      println(s"Feeds descargados con éxito: ${feedsSuccess}")
      println(s"Feeds fallidos: ${feedsFailed}")
      println(s"Posts descargados con éxito: ${postsSuccess}")
      println(s"Posts fallidos: ${postsFailed}")
      println(s"Posts filtrados: ${postsFiltered}")
      println(s"Longitud promedio (caracteres) de posts filtrados: ${avgChars}")

    println(Formatters.formatProcessingStats(stats))
    println()

    // Check if we have any posts to process
    if (filteredPosts.isEmpty) {
      println("Error: No valid posts downloaded after filtering")
      return
    }

    // Load dictionaries
    val dictionary = Dictionary.loadAll(cmdArgs.entitiesDir)

    // Broadcast dictionary for use in RDD transformations
    val dictBroadcast = sc.broadcast(dictionary)

    val results = postsRDD
      .filter(post => post.title.nonEmpty && post.selftext.nonEmpty)
      .flatMap { post =>
        val combinedText = post.title + " " + post.selftext
        try {
          Analyzer.detectEntities(combinedText, dictBroadcast.value)
        } catch {
          case e: Exception =>
            println(s"Warning: Failed to detect entities in a post (${e.getMessage})")
            List.empty[NamedEntity]
        }
      }
      .map { entity => ((entity.entityType, entity.text), 1) }
      .reduceByKey(_ + _)
      .sortBy({ case ((tipo, nombre), count) => (-count, tipo) })
      .collect()

    // Mostrar resultados
    results.foreach { case ((tipo, nombre), count) =>
      println(s"[$tipo] $nombre: $count apariciones")
    }
  }
}
