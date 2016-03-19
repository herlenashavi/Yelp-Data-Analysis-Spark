val filePath = "hdfs://cshadoop1/yelpdatafall/business/business.csv"
val location = readLine("Enter location")
val logData = sc.textFile(filePath)
val lines = logData.filter(line => line.contains(location)).map(_.split("\\^")(0)).distinct
lines.foreach(println)

