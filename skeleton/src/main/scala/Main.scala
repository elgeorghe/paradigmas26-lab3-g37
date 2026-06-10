import org.apache.spark.sql.SparkSession

object Main {
  def main(args: Array[String]): Unit = {
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
    val spark = SparkSession.builder()
      .appName("RedditNER")
      .master("local[*]")
      .getOrCreate()

    case class FeedResult(success: Boolean, posts: List[Post], filteredPosts: List[Post])

    try {
      val sc = spark.sparkContext
      val subscriptionRdd = sc.parallelize(subscriptions) //crea una RDD

      val feedResults = subscriptionRdd.map { subscription => //crea nuevo RDD (feedResults)
        FileIO.downloadFeed(subscription.url) match {
          case Some(feedJson) =>
            try {
              val posts = JsonParser.parsePosts(feedJson, subscription.name) //parsea el json
              val filteredPosts = Analyzer.filterEmptyPosts(posts) // filtra posts vacios
              FeedResult(success = true, posts = posts, filteredPosts = filteredPosts) // crea objeto nuevo donde guarda posts y posts analizados
            } catch {
              case _: Exception =>
                println(s"Warning: Failed to parse posts from '${subscription.name}' (${subscription.url})")
                FeedResult(success = true, posts = List.empty, filteredPosts = List.empty)
            }
          case None =>
            println(s"Warning: Failed to download from '${subscription.name}' (${subscription.url})")
            FeedResult(success = false, posts = List.empty, filteredPosts = List.empty)
        }
      }.cache()

      val collectedFeedResults = feedResults.collect().toList
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

      // Detect entities in all posts (combine title and selftext)
      val allEntities = filteredPostsList.flatMap { post =>
        val combinedText = post.title + " " + post.selftext
        Analyzer.detectEntities(combinedText, dictionary)
      }

      // Count entities
      val entityCounts = Analyzer.countEntities(allEntities)
      val typeStats = Analyzer.countByType(allEntities)

      println(Formatters.formatTypeStats(typeStats))
      println()
      println(Formatters.formatEntityStats(entityCounts, cmdArgs.topK))
    } finally {
      spark.stop()
    }
  }
}

