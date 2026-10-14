import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}

// Input line: eventId,bookingId,resourceType,resourceId,action
// e.g.        E001,B001,HOTEL,HOTEL101,BOOK     (action = BOOK | CANCEL)
case class BookingEvent(
  eventId: String,
  bookingId: String,
  resourceType: String,
  resourceId: String,
  eventType: String
)

object Day28RealtimeBooking {

  def parse(line: String): Option[BookingEvent] = {
    val p = line.trim.split(",").map(_.trim)
    if (p.length != 5) None
    else {
      val action = p(4).toUpperCase
      if (action == "BOOK" || action == "CANCEL")
        Some(BookingEvent(p(0), p(1), p(2).toUpperCase, p(3).toUpperCase, action))
      else None
    }
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("Day28RealtimeBooking")
      .setIfMissing("spark.master", "local[2]")

    val ssc = new StreamingContext(conf, Seconds(10))
    ssc.checkpoint("checkpoint/day28") // required by updateStateByKey
    ssc.sparkContext.setLogLevel("WARN")

    val spark = SparkSession.builder().config(ssc.sparkContext.getConf).getOrCreate()
    import spark.implicits._

    // Broadcast reference data: resourceId -> (resourceType, capacity)
    val reference = Map(
      "HOTEL101"  -> ("HOTEL", 10),
      "FLIGHT101" -> ("FLIGHT", 100),
      "BUS101"    -> ("BUS", 40)
    )
    val refBc = ssc.sparkContext.broadcast(reference)

    // Source -> parsed, validated events (unknown resources dropped).
    // Persisted because several outputs below reuse the same stream.
    val events = ssc
      .socketTextStream("localhost", 9999, StorageLevel.MEMORY_AND_DISK_SER)
      .flatMap(line => parse(line))
      .filter(e => refBc.value.contains(e.resourceId))
      .persist(StorageLevel.MEMORY_ONLY)

    // 1. Pair RDD: events per resource in this batch
    events
      .map(e => ((e.resourceType, e.resourceId), 1))
      .reduceByKey(_ + _)
      .foreachRDD { rdd =>
        rdd.collect().foreach { case ((t, id), n) =>
          println(s"EVENT COUNT type=$t, resource=$id, count=$n")
        }
      }

    // 2. Booking state per bookingId: BOOK = +1, CANCEL = -1 (never below 0)
    val bookingState = events
      .map(e => (e.bookingId, if (e.eventType == "BOOK") 1 else -1))
      .updateStateByKey[Int] { (deltas: Seq[Int], prev: Option[Int]) =>
        Some(math.max(0, prev.getOrElse(0) + deltas.sum))
      }

    bookingState.foreachRDD { rdd =>
      rdd.collect().sortBy(_._1).foreach { case (id, n) =>
        val status = if (n > 0) "ACTIVE" else "CANCELLED"
        println(s"BOOKING STATE id=$id, status=$status")
      }
    }

    // 3. Occupancy state per resource: running count of active bookings
    val occupancyState = events
      .map(e => (e.resourceId, if (e.eventType == "BOOK") 1 else -1))
      .updateStateByKey[Int] { (deltas: Seq[Int], prev: Option[Int]) =>
        Some(math.max(0, prev.getOrElse(0) + deltas.sum))
      }

    // 4. Spark SQL report: join running occupancy with broadcast capacity data
    occupancyState.foreachRDD { rdd =>
      val capacityDF = refBc.value.toSeq
        .map { case (id, (t, cap)) => (id, t, cap) }
        .toDF("resourceId", "resourceType", "capacity")
      val occupancyDF = rdd.toDF("resourceId", "occupied")

      capacityDF.createOrReplaceTempView("capacity")
      occupancyDF.createOrReplaceTempView("occupancy")

      println("\n===== OCCUPANCY & AVAILABILITY REPORT =====")
      spark.sql("""
        SELECT c.resourceType, c.resourceId, c.capacity,
               COALESCE(o.occupied, 0) AS occupied,
               GREATEST(c.capacity - COALESCE(o.occupied, 0), 0) AS available,
               ROUND(100.0 * COALESCE(o.occupied, 0) / c.capacity, 1) AS occupancyPct,
               CASE WHEN COALESCE(o.occupied, 0) > c.capacity THEN 'OVERBOOKED'
                    WHEN COALESCE(o.occupied, 0) = c.capacity THEN 'FULL'
                    ELSE 'OK' END AS status
        FROM capacity c
        LEFT JOIN occupancy o ON c.resourceId = o.resourceId
        ORDER BY c.resourceType, c.resourceId
      """).show(false)
    }

    // 5. Window: bookings vs cancellations in the last 60s, sliding every 30s
    events
      .map(e => ((e.resourceType, e.resourceId),
        if (e.eventType == "BOOK") (1, 0) else (0, 1)))
      .reduceByKeyAndWindow(
        (a: (Int, Int), b: (Int, Int)) => (a._1 + b._1, a._2 + b._2),
        Seconds(60), Seconds(30))
      .foreachRDD { rdd =>
        println("\n===== BOOKING ACTIVITY: LAST 60 SECONDS =====")
        rdd.collect().sortBy(_._1.toString).foreach { case ((t, id), (b, c)) =>
          println(s"$t - $id -> bookings=$b, cancellations=$c")
        }
      }

    println("Day 28: Real-Time Booking System")
    println("Batch: 10s | Window: 60s | Slide: 30s | Socket: localhost:9999")

    ssc.start()
    ssc.awaitTermination()
  }
}
