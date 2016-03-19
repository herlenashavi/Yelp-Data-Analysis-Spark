val reviewPath = "hdfs://cshadoop1/yelpdatafall/review/review.csv"
val businessPath = "hdfs://cshadoop1/yelpdatafall/business/business.csv"

val t0 = System.nanoTime()
val businessRatings = sc.textFile(reviewPath).map { line =>
        val lineArr = line.split("\\^")
	        (lineArr(2), lineArr(3).toDouble)
	}
val busAvgRatings = businessRatings.groupByKey().map(data => { val avg = data._2.sum / data._2.size; (avg,data._1)})
val result = busAvgRatings.sortBy(_._2).sortByKey(false,1).take(10)
val rest = result.map(_.swap)

val businessMap = sc.textFile(businessPath).map { line =>
        val lineArr = line.split("\\^")
	        (lineArr(0), lineArr(1).concat(lineArr(2)))
	}.distinct.collectAsMap
val bc = sc.broadcast(businessMap)

val joined = rest.map(v => (v._1, bc.value(v._1)))
println("")
println("")
joined.sortBy(_._1).foreach{println}
					
val t1 = System.nanoTime()
println("Elapsed time: " + (t1 - t0) + "ns")
				
