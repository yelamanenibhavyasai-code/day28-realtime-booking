#!/bin/bash
# Usage (Terminal 1):  ./scripts/feeder.sh | nc -lk 9999
# Then start the app in Terminal 2. Feeder waits 20s for the app to connect.
sleep 20
echo "E001,B001,HOTEL,HOTEL101,BOOK"
echo "E002,B002,HOTEL,HOTEL101,BOOK"
echo "E003,B003,FLIGHT,FLIGHT101,BOOK"
sleep 12
echo "E004,B001,HOTEL,HOTEL101,CANCEL"
echo "E005,B004,BUS,BUS101,BOOK"
echo "E006,B005,BUS,UNKNOWN9,BOOK"      # unknown resource -> dropped
echo "bad,line"                          # malformed -> dropped
sleep 90
