object Dictionary {

  /**
   * Load entities from a dictionary file and create instances of the specified type.
   * @param filePath path to dictionary file (e.g., "data/people.txt")
   * @param entityType type of entity to create ("Person", "University", etc.)
   * @return Option containing list of entities, None if file missing
   */
  def loadFromFile(filePath: String, entityType: String): Option[List[NamedEntity]] = {
    FileIO.readDictionaryFile(filePath).map { lines =>
      lines.map { name =>
        entityType match {
          case "Person"              => new Person(name)
          case "Organization"        => new Organization(name)
          case "University"          => new University(name)
          case "Place"               => new Place(name)
          case "Technology"          => new Technology(name)
          case "ProgrammingLanguage" => new ProgrammingLanguage(name)
          case _                     => new Person(name) // fallback
        }
      }
    }
  }

  /**
   * Load all dictionary files and combine into a single list.
   * Prints warnings for missing dictionary files and continues with others.
   * @param entitiesDir path to directory containing entity files
   * @return combined list of all entities from all successfully loaded dictionaries
   */
  def loadAll(entitiesDir: String): List[NamedEntity] = {
    // Check if entities directory exists
    val dataDir = new java.io.File(entitiesDir)
    if (!dataDir.exists() || !dataDir.isDirectory) {
      println(s"Error: entities directory '$entitiesDir' not found")
      return List()
    }

    val peoplePath = s"$entitiesDir/people.txt"
    val universitiesPath = s"$entitiesDir/universities.txt"
    val languagesPath = s"$entitiesDir/languages.txt"
    val organizationsPath = s"$entitiesDir/organizations.txt"
    val placesPath = s"$entitiesDir/places.txt"

    val peopleOpt = loadFromFile(peoplePath, "Person") match {
      case Some(v) => Some(v)
      case None => println(s"Warning: Could not load $peoplePath"); None
    }

    val universitiesOpt = loadFromFile(universitiesPath, "University") match {
      case Some(v) => Some(v)
      case None => println(s"Warning: Could not load $universitiesPath"); None
    }

    val languagesOpt = loadFromFile(languagesPath, "ProgrammingLanguage") match {
      case Some(v) => Some(v)
      case None => println(s"Warning: Could not load $languagesPath"); None
    }

    val organizationsOpt = loadFromFile(organizationsPath, "Organization") match {
      case Some(v) => Some(v)
      case None => println(s"Warning: Could not load $organizationsPath"); None
    }

    val placesOpt = loadFromFile(placesPath, "Place") match {
      case Some(v) => Some(v)
      case None => println(s"Warning: Could not load $placesPath"); None
    }

    peopleOpt.getOrElse(List()) :::
      universitiesOpt.getOrElse(List()) :::
      languagesOpt.getOrElse(List()) :::
      organizationsOpt.getOrElse(List()) :::
      placesOpt.getOrElse(List())
  }
}
