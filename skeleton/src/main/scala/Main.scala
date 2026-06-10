import org.apache.spark.sql.SparkSession
import org.apache.logging.log4j.{Level, LogManager}
import org.apache.logging.log4j.core.config.Configurator



object Main {
  def main(args: Array[String]): Unit = {
    Configurator.setRootLevel(Level.ERROR)

    val spark = SparkSession.builder()
      .appName("RedditNER")
      .master("local[*]")
      .config("spark.ui.showConsoleProgress", "false")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    // Parse command-line arguments
    val cmdArgs = CommandLineArgs.parse(args) match {
      case Some(parsed) => parsed
      case None => return // scopt prints error messages
    }

    // Load subscriptions
    val subscriptionResult = FileIO.readSubscriptions(cmdArgs.subscriptionFile)
    val subscriptions = subscriptionResult match {
      case Left(errorMessage) =>
        println(errorMessage)
        return
      case Right(list) =>
        list
    }

    if (subscriptions.isEmpty) {
      println("Error: No valid subscriptions found")
      return
    }

    // Spark setup and subscription RDD
    /*
    val spark = SparkSession.builder()
      .appName("RedditNER")
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.kryo.registrationRequired", "false")
      .config("spark.driver.extraJavaOptions", "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED")
      .config("spark.executor.extraJavaOptions", "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED")
      .getOrCreate()
*/
    case class FeedResult(success: Boolean, posts: List[Post], filteredPosts: List[Post])

    try {
      // Download and parse feeds locally (avoid Spark serialization issues)
      val collectedFeedResults = subscriptions.map { subscription =>
        FileIO.downloadFeed(subscription.url) match {
          case Some(feedJson) =>
            try {
              val posts = JsonParser.parsePosts(feedJson, subscription.name)
              val filteredPosts = Analyzer.filterEmptyPosts(posts)
              FeedResult(success = true, posts = posts, filteredPosts = filteredPosts)
            } catch {
              case _: Exception =>
                println(s"Warning: Failed to parse posts from '${subscription.name}' (${subscription.url})")
                FeedResult(success = true, posts = List.empty, filteredPosts = List.empty)
            }
          case None =>
            println(s"Warning: Failed to download from '${subscription.name}' (${subscription.url})")
            FeedResult(success = false, posts = List.empty, filteredPosts = List.empty)
        }
      }
      val allPosts = collectedFeedResults.flatMap(_.posts)
      val filteredPosts = collectedFeedResults.flatMap(_.filteredPosts)

      val feedsSuccess = collectedFeedResults.count(_.success)
      val feedsFailed = collectedFeedResults.count(!_.success)
      val postsSuccess = allPosts.length
      val postsFailed = collectedFeedResults.count(_.posts.isEmpty)
      val filteredPostsCount = filteredPosts.length
      val postsFiltered = postsSuccess - filteredPostsCount
      val totalChars = filteredPosts.map(post => post.title.length + post.selftext.length).sum
      val avgChars = if (filteredPostsCount > 0) totalChars / filteredPostsCount else 0

      val stats = Map( //crea un diccionario inmutable
        "feedsSuccess" -> feedsSuccess,
        "feedsFailed" -> feedsFailed,
        "postsSuccess" -> postsSuccess,
        "postsFailed" -> postsFailed,
        "postsFiltered" -> postsFiltered,
        "avgChars" -> avgChars
      )

      println(Formatters.formatProcessingStats(stats))
      println()

      if (filteredPostsCount == 0) {
        println("Error: No valid posts downloaded after filtering")
        return
      }

      val filteredPostsList = filteredPosts

      // Load dictionaries
      val dictionary = Dictionary.loadAll(cmdArgs.entitiesDir)

      // Run entity detection and aggregation locally to avoid Spark serialization issues
      val detectedEntities = filteredPostsList.flatMap { post =>
        val combinedText = post.title + " " + post.selftext
        Analyzer.detectEntities(combinedText, dictionary)
      }

      val results = detectedEntities
        .groupBy(e => (e.entityType, e.text))
        .map { case (k, list) => (k, list.size) }
        .toList
        .sortBy { case ((tipo, nombre), count) => (-count, tipo) }
        .toArray

      // Mostrar resultados: formato de entidades y estadísticas por tipo
      val entityCountsMap: Map[(String, String), Int] = results.toMap

      // Print top entities using Formatters
      println(Formatters.formatEntityStats(entityCountsMap, cmdArgs.topK))
      println()

      // Compute type stats
      val typeCounts: Map[String, Int] = results
        .groupBy { case ((tipo, _), _) => tipo }
        .map { case (tipo, list) => tipo -> list.map(_._2).sum }

      val totalEntities = typeCounts.values.sum
      val typeStatsWithTotal = typeCounts + ("total" -> totalEntities)
      println(Formatters.formatTypeStats(typeStatsWithTotal))
    } finally {
      spark.stop()
    }
  }
}

