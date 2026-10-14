# Day 28 - Real-Time Booking (Spark Streaming + Spark SQL)

Event: `eventId,bookingId,resourceType,resourceId,action` (BOOK | CANCEL)

| Requirement | Implementation |
|---|---|
| Process hotel/flight/bus events | socket stream -> `parse` -> validated `BookingEvent` |
| Booking/cancellation state | `updateStateByKey` per bookingId (ACTIVE / CANCELLED) |
| Occupancy & availability | `updateStateByKey` per resourceId; available = capacity - occupied |
| Pair RDD + window | `reduceByKey` per batch; `reduceByKeyAndWindow` 60s/30s |
| Broadcast reference data | resourceId -> (type, capacity) |
| Spark SQL report | capacity table LEFT JOIN occupancy table, with occupancy % and status |

## Run
Terminal 1: `./scripts/feeder.sh | nc -lk 9999`   (or `nc -lk 9999` and type lines)
Terminal 2: `sbt "runMain Day28RealtimeBooking"`

Always start `nc` first - the Spark app is the client.
