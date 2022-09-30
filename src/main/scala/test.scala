object test extends App{

  import org.apache.spark.sql.SparkSession
  val spark = SparkSession.builder()
    .appName("Covid19")
    .master("local")
    .config("spark.sql.warehouse.dir", "file:///c:/tmp/spark-warehouse")
    .getOrCreate()

  val cvd_cities = spark.read.option("header",true).csv("src/main/resources/brazil_covid19_cities.csv")
  val cvd_state = spark.read.option("header",true).csv("src/main/resources/brazil_covid19.csv")
  val state = cvd_state.select("region","state").distinct()
  val cvd_dest = cvd_cities.join(state,Seq("state")).selectExpr("state","date","name","code","region",
    "cast(cases as int)","cast(deaths as int)")
    .groupBy("date","region","state").sum("cases","deaths")
    .withColumnRenamed("sum(cases)","cases")
    .withColumnRenamed("sum(deaths)","deaths")


  //  number of source rows :12258
  println("number of source rows: "+cvd_state.count())

  //  number of destination rows :11421
  println("number of destination rows: "+cvd_dest.count())

  //  number of rows present in source but not in destination (= key present in source but not in destination) : 837
  val source = cvd_state.join(cvd_dest,Seq("state","date"),"leftAnti")
  println("number of rows present in source but not in destination: "+source.count())

  //  number of rows present in destination but not in source (= key present in destination but not in source) : 0
  val dest = cvd_dest.join(cvd_state,Seq("date","state"),"leftAnti")
  println("number of rows present in destination: "+dest.count())

  //  number of rows identical in source and destination (= having same cases & deaths values for the given key) : 7384
  val idntCnt = cvd_dest.join(cvd_state,Seq("date","state","region","cases","deaths"))
  println("number of rows identical in source and destination: "+ idntCnt.count())

  //  number of rows with different values in source and destination ( = having at least a different cases & deaths value for a given key) :11421
  val diffCnt = cvd_state.join(cvd_dest,cvd_state("date")===cvd_dest("date") && cvd_state("state")===cvd_dest("state")
    && cvd_state("cases")!=cvd_dest("cases") && cvd_state("deaths")!=cvd_dest("deaths"))
  println("number of rows with different values in source and destination: "+diffCnt.count())

println("checking git stash and git reset")
}