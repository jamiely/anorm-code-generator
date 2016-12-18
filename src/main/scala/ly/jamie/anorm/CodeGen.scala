package ly.jamie.anorm

object StringUtils {

  /**
   * Takes a camel cased identifier name and returns an underscore separated
   * name
   *
   * Example:
   *     camelToUnderscores("thisIsA1Test") == "this_is_a_1_test"
   */
  def camelToUnderscores(name: String) = "[A-Z\\d]".r.replaceAllIn(name, {m =>
    "_" + m.group(0).toLowerCase()
  })

  /*
   * Takes an underscore separated identifier name and returns a camel cased one
   *
   * Example:
   *    underscoreToCamel("this_is_a_1_test") == "thisIsA1Test"
   */

  def underscoreToCamel(name: String) = "_([a-z\\d])".r.replaceAllIn(name, {m =>
    m.group(1).toUpperCase()
  })
}

object ClassField {
  private val reOption = """^Option\[(\w+)\]$""".r
  def getBasicDataType(dataType: String) =
    if(dataType.startsWith("Option")) {
      dataType match {
        case reOption(basic) => basic
        case p => throw new Exception(s"Unrecognized pattern $p")
      }
    }
    else dataType
}

case class ClassField(name: String, dataType: String, emptyValue: String) {
  val isOption = dataType.startsWith("Option")
  lazy val basicDataType = ClassField.getBasicDataType(dataType)
}

object StatementGenerator {
  import StringUtils._

  def sqlFieldDataTypeToClassFieldDataType(field: SQLField): String = {
    val simple = sqlFieldDataTypeToClassFieldDataType(field.basicDataType)

    if(field.nullable) s"Option[$simple]"
    else simple
  }

  def getEmptyValueFromClassType(typ: String): String =
    if(typ.startsWith("Option")) "None"
    else typ match {
      case "Boolean" => "false"
      case "Date" => "new Date(0)"
      case "Int" => "0"
      case "BigDecimal" => "BigDecimal(0)"
      case _ => """"""""
    }

  def sqlFieldDataTypeToClassFieldDataType(typ: String): String = typ match {
    case "boolean" => "Boolean"
    case "timestamp" | "date" | "datetime" => "Date"
    case "int" => "Int"
    case "decimal" => "BigDecimal"
    case "varchar" => "String"
    case "text" => "String"
    case a => throw new Exception(s"Unknown type $a")
  }

  def fieldToClassField(sqlField: SQLField): ClassField = {
    val field = sqlFieldDataTypeToClassFieldDataType(sqlField)
    ClassField(
      underscoreToCamel(sqlField.name),
      field,
      getEmptyValueFromClassType(field)
    )
  }
}

case class GeneratorConfig(
  includeTrait: Boolean = true,
  connectionName: String = "default",
  overrideClassName: Option[String] = None,
  debug: Boolean = false,
  packageName: Option[String] = None
)

object GeneratorConfig {
  val default = GeneratorConfig(includeTrait = true)
}

case class StatementGenerator(table: Table, config: GeneratorConfig = GeneratorConfig.default) {
  import StatementGenerator._
  import StringUtils._

  val sqlFields = table.fields

  lazy val caseClassName = config.overrideClassName.getOrElse(underscoreToCamel(table.name).capitalize)
  lazy val classFields = sqlFields.map(fieldToClassField)
  lazy val classFieldsStr = classFields.map { f =>
    s"  ${f.name}: ${f.dataType}"
  }.mkString(",\n")

  lazy val caseClassStr = s"""case class $caseClassName (
    |$classFieldsStr
    |)""".stripMargin +
    (
      if(config.includeTrait) {
        s" extends $traitName"
      } else {
        ""
      }
    )

  lazy val emptyClassDefn = classFields.map ( f =>
    s"${f.name} = ${f.emptyValue}"
    )

  lazy val traitName = s"${caseClassName}Like"

  lazy val traitFieldsStr = classFields.map { f =>
    s"  val ${f.name}: ${f.dataType}"
  }

  lazy val traitDefnStr = s"""trait $traitName {
    |${traitFieldsStr.mkString("\n")}
    |}""".stripMargin

  lazy val singletonClassStr = s"""object $caseClassName {
    |  val empty = $caseClassName(
    |${emptyClassDefn.map("    " + _).mkString(",\n")}
    |  )
    |}""".stripMargin

  lazy val sqlFieldNames = table.fields.map{_.name}
  lazy val classFieldNames = classFields.map { _.name }

  lazy val insertStatement = {
    s"""INSERT INTO ${table.name} (
      |${sqlFieldNames.map{ "  " + _ }.mkString(",\n")}
      |)
      |VALUES (
      |${classFieldNames.map{ "  {" + _ + "}" }.mkString(",\n")}
      |)""".stripMargin
  }

  lazy val wherePrimaryKey = {
    table.primaryKey.map { f =>
      s"${f.name} = {${underscoreToCamel(f.name)}}"
    }.map("  " + _).mkString(" AND\n")
  }


  lazy val updateStatement = {
    val list = sqlFields.map { f =>
      s"  ${f.name} = {${underscoreToCamel(f.name)}}"
    }.mkString(",\n")
    s"""UPDATE ${table.name} SET
      |$list
      |WHERE
      |$wherePrimaryKey""".stripMargin
  }

  lazy val parserName = caseClassName + "Parser"

  def getAnormCombinator(field: SQLField): String = {
    val name = field.name
    val fun = field.basicDataType match {
      case "int" => "int"
      case "decimal" => s"bigDecimal"
      case "boolean" => "bool"
      case "date" | "datetime" | "timestamp" => "date"
      case "varchar" => "str"
      case "text" => "str"
      case a => throw new Exception(s"Unexpected data type for anorm combinator $a")
    }
    val s = s"""$fun("$name")"""
    if(field.nullable) s + ".?"
    else s
  }

  lazy val parserDefinition = {
    val combinators = sqlFields.map { f =>
      "    " + getAnormCombinator(f)
    }.mkString(" ~\n")

    val namedParamList = classFieldNames.map { n =>
      s"""    "$n" -> v.$n"""
    }.mkString(",\n")

    val bigDecimalParser = s"""
    |  def bigDecimal(column: String)(implicit c: Column[String]): RowParser[BigDecimal] =
    |    SqlParser.get[java.math.BigDecimal](column).map(BigDecimal.apply)
    |""".stripMargin

    val parser = s"""
      |$combinators map flatten map ($caseClassName.apply _).tupled""".stripMargin
    s"""object $parserName extends RowParser[$caseClassName] {
    |
    |  import anorm.SqlParser._
    |
    |  private lazy val parser =$parser
    |${if(includeBigDecimalParser) bigDecimalParser else ""}
    |  def apply(row: Row): SqlResult[$caseClassName] = parser(row)
    |
    |  def getNamedParams(v: $caseClassName): Seq[NamedParameter] = Seq(
    |$namedParamList
    |  )
    |}""".stripMargin
  }

  lazy val includeBigDecimalParser = sqlFields.exists(f => f.basicDataType == "decimal")

  lazy val selectAllStatement = s"""
    |SELECT
    |${sqlFieldNames.map("  " + _).mkString(",\n")}
    |FROM ${table.name}""".stripMargin

  lazy val selectOneStatement = s"""
    |$selectAllStatement
    |WHERE
    |$wherePrimaryKey""".stripMargin

  val daoSQLDefn = {
    def tripleQuote(str: String) = s"""\"\"\"$str\"\"\""""
    def indentStr(str: String) =
      tripleQuote(str.lines.map { line =>
        "          " + line
      }.mkString("\n").trim)

    val primaryKeyArgsNoType = table.primaryKey.map { f =>
      val cfDataType = sqlFieldDataTypeToClassFieldDataType(f)
      s"${underscoreToCamel(f.name)}"
    }.mkString(", ")

    val primaryKeyArgs = table.primaryKey.map { f =>
      val cfDataType = sqlFieldDataTypeToClassFieldDataType(f)
      s"${underscoreToCamel(f.name)}: $cfDataType"
    }.mkString(", ")

    val primaryKeyValues = table.primaryKey.map { f =>
      val name = underscoreToCamel(f.name)
      s""""$name" -> $name"""
    }.mkString(", ")

    val daoTraitName = caseClassName + "DAOLike"
    val daoDBTraitName = caseClassName + "DBDAOLike"
    val daoDBName = caseClassName + "DBDAO"

    s"""trait $daoTraitName {
    |  def insert(v: $caseClassName): Future[$caseClassName]
    |  def update(v: $caseClassName): Future[$caseClassName]
    |  def find($primaryKeyArgs): Future[$caseClassName]
    |  def findOpt($primaryKeyArgs): Future[Option[$caseClassName]]
    |  def all: Future[Seq[$caseClassName]]
    |}
    |
    |trait $daoDBTraitName extends $daoTraitName {
    |
    |  val connectionName = "${config.connectionName}"
    |
    |  protected def withConnection[T](f: java.sql.Connection => T): Future[T] = Future {
    |    DB.withConnection(connectionName) { conn =>
    |      f(conn)
    |    }
    |  }
    |
    |  override def insert(v: $caseClassName): Future[$caseClassName] =
    |    withConnection { implicit conn =>
    |      SQL(${indentStr(insertStatement)})
    |        .on($parserName.getNamedParams(v): _*)
    |        .execute()
    |      v
    |    }
    |
    |  override def update(v: $caseClassName): Future[$caseClassName] =
    |    withConnection { implicit conn =>
    |      SQL(${indentStr(updateStatement)})
    |        .on($parserName.getNamedParams(v): _*)
    |        .execute()
    |      v
    |    }
    |
    |  def find($primaryKeyArgs): Future[$caseClassName] =
    |    withConnection { implicit conn =>
    |      SQL(${indentStr(selectOneStatement)})
    |        .on($primaryKeyValues)
    |        .as($parserName.single)
    |    }
    |
    |  def findOpt($primaryKeyArgs): Future[Option[$caseClassName]] =
    |    find($primaryKeyArgsNoType).map(Option.apply).recover { case e => None }
    |
    |  def all: Future[Seq[$caseClassName]] =
    |    withConnection { implicit conn =>
    |      SQL(selectAll).as($parserName.*)
    |    }
    |
    |  protected val selectAll = ${indentStr(selectAllStatement)}
    |}
    |
    |object $daoDBName extends $daoDBTraitName {
    |}""".stripMargin
  }

  lazy val anormSQLInterpolation = s"""
    |SQL"INSERT INTO ${table.name} (${sqlFieldNames.mkString(",")}) VALUES (${classFieldNames.map("$" + _).mkString(", ")})"
    |
    |SQL"UPDATE ${table.name} SET ${sqlFieldNames.zip(classFieldNames).map{case (a,b) => a + " = $" + b}.mkString(", ")} WHERE = $wherePrimaryKey"
    |""".stripMargin


  lazy val allDefinitions = {
    (config.packageName match {
        case Some(name) => Seq(s"package $name")
        case _ => Seq.empty
    }) ++
    Seq(Seq(
      "import anorm._",
      "import java.util.Date",
      "import scala.concurrent.Future",
      "import play.api.libs.concurrent.Execution.Implicits.defaultContext",
      "import play.api.db.DB",
      "import play.api.Play.current"
    ).mkString("\n")) ++
    (if(config.includeTrait) {
      Seq(traitDefnStr)
    }
    else {
      Seq.empty[String]
    }) ++
    Seq(
      caseClassStr,
      singletonClassStr,
      //selectOneStatement,
      //selectAllStatement,
      //insertStatement,
      //updateStatement,
      parserDefinition,
      daoSQLDefn,
      anormSQLInterpolation
    )
  }.mkString("\n\n")
}

case class SQLField(
  name: String,
  dataType: String,
  nullable: Boolean = true,
  defaultValue: Option[String] = None
) {
  val basicDataType = dataType.replaceAll("""\(.*""", "")
}

case class Table(
  name: String,
  schema: String,
  fields: Seq[SQLField],
  primaryKey: Seq[SQLField]
) {
  def addField(field: SQLField): Table =
    copy(fields = fields :+ field)
  def addPrimaryKey(field: SQLField): Table =
    copy(primaryKey = primaryKey :+ field)
}

object Table {
  val defaultSchema = "public"
  val empty = Table(name = "", schema = defaultSchema, fields = Seq.empty,
    primaryKey = Seq.empty)
}

object ConfigParser {
  import scopt._

  val parser = new scopt.OptionParser[GeneratorConfig]("anormCodeGen") {
    head("anormCodeGen", "0.x")

    opt[String]('c', "connection-name").action( (value, c) =>
      c.copy(connectionName = value)).text("connection name as defined in your conf file")

    opt[String]('n', "class-name").action( (value, c) =>
      c.copy(overrideClassName = Option(value))).text("the class name to use instead of the one generated from the table name")

    opt[String]('p', "package").action((value, c) =>
      c.copy(packageName = Option(value))).text("the package name to add")

    opt[Unit]("exclude-trait").action( (_, c) =>
      c.copy(includeTrait = false)).text("Whether to include a trait definition for the case class")

    opt[Unit]("debug").action((_, c) =>
      c.copy(debug = true)).text("enable debugging")

    help("help").text("Prints this usage text")
  }
}

case class CodeGen(args: Array[String]) {
  private val createTableRE = """.*CREATE\s+TABLE\s+(\w+\.)?(\w+).*""".r
  private val fieldRE = """\s*(\w+)\s+(int|text|character varying\(\d+\)|varchar\(\d+\)|numeric\([^\)]+\)|decimal\([^\)]+\)|boolean|timestamp)(\s+(?:NOT\s+)NULL)?(\s+default\s+\w+)?.*""".r
  private val fieldPrimaryKeyRE = """\s*(\w+)\s+(int|text|character varying\(\d+\)|varchar\(\d+\)|numeric\([^\)]+\)|decimal\(\[^\)]+\)|boolean|timestamp)\s+PRIMARY\s+KEY.*""".r

  private lazy val config = ConfigParser.parser.parse(args, GeneratorConfig()).get

  protected def normalizeDataType(dataType: String): String =
    dataType.replaceAll("character varying", "varchar")
      .replaceAll("numeric", "decimal")

  def debug(msg: String) =
    if(config.debug) println(msg)
  def run() {

    args.foreach(debug)
    debug("STDIN:")

    val lines = io.Source.stdin.getLines
    val table = lines.foldLeft(Table.empty) {
      case (table, line) => line match {
        case createTableRE(schema, tableName) =>
          table.copy(
            name = tableName,
            schema = Option(schema).getOrElse(Table.defaultSchema).replace(".", "")
          )
        case fieldPrimaryKeyRE(fieldName, dataType) =>
          val f = SQLField(
              name = fieldName,
              dataType = normalizeDataType(dataType),
              nullable = false)

          table.addField(f).addPrimaryKey(f)

        case fieldRE(fieldName, dataType, nullStr, defaults) =>
          debug(s"field=$fieldName type=$dataType null=$nullStr default=$defaults")
          val f = SQLField(
            name = fieldName,
            dataType = normalizeDataType(dataType),
            nullable = Option(nullStr).forall(! _.toLowerCase.contains("not")),
            defaultValue = Option(defaults).map(_.replace("""default\s+""", ""))
              .filter(_.nonEmpty)
          )
          debug(f + ": " + f.basicDataType)
          table.addField(f)
        case line =>
          debug("> " + line)
          table
      }
    }

    debug("// " + table)
    debug("\n\n\n")
    println(StatementGenerator(table, config).allDefinitions)
  }
}

object CodeGen {
  def main(args: Array[String]) {
    CodeGen(args).run()
  }
}
