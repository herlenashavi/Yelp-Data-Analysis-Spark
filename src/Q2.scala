val filePath = "hdfs://cshadoop1/yelpdatafall/review/review.csv"
val logData = sc.textFile(filePath)
val t0 = System.nanoTime()
val businessRatings = sc.textFile(filePath).map { line =>
	val lineArr = line.split("\\^")
	(lineArr(2), lineArr(3).toDouble)
}

val busAvgRatings = businessRatings.groupByKey().map(data => { val avg = data._2.sum / data._2.size; (avg,data._1)})
val result = busAvgRatings.sortBy(_._2).sortByKey(false,1).take(10)
println()
result.foreach{
	case(key,value) =>
	println(value,key)
}
val t1 = System.nanoTime()
println("Elapsed time: " + (t1 - t0) + "ns")
