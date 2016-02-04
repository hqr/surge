#!/bin/bash
set -x
BUILD=`git rev-parse --short HEAD`
DT=${1:-400}
# 90x90
sleep 2
go run cmd/ck.go -ttr 10ms -m 5 -gateways 90 -servers 90 -build $BUILD -diskthroughput $DT
cd /tmp
zip `basename log*.csv | cut -d"." -f1` log*.csv
rm log*.csv
cd -
sleep 2
go run cmd/ck.go -ttr 10ms -m 6 -gateways 90 -servers 90 -build $BUILD -diskthroughput $DT
cd /tmp
zip `basename log*.csv | cut -d"." -f1` log*.csv
rm log*.csv
cd -
sleep 2
go run cmd/ck.go -ttr 10ms -m 7 -gateways 90 -servers 90 -build $BUILD -diskthroughput $DT
cd /tmp
zip `basename log*.csv | cut -d"." -f1` log*.csv
rm log*.csv
cd -
sleep 2
go run cmd/ck.go -ttr 10ms -m 8 -gateways 90 -servers 90 -build $BUILD -diskthroughput $DT
cd /tmp
zip `basename log*.csv | cut -d"." -f1` log*.csv
rm log*.csv
cd -
# 90x90 16K
go run cmd/ck.go -ttr 10ms -m 5 -gateways 90 -servers 90 -build $BUILD -chunksize 16 -diskthroughput $DT
cd /tmp
zip `basename log*.csv | cut -d"." -f1` log*.csv
rm log*.csv
cd -
sleep 2
go run cmd/ck.go -ttr 10ms -m 6 -gateways 90 -servers 90 -build $BUILD -chunksize 16 -diskthroughput $DT
cd /tmp
zip `basename log*.csv | cut -d"." -f1` log*.csv
rm log*.csv
cd -
sleep 2
go run cmd/ck.go -ttr 10ms -m 7 -gateways 90 -servers 90 -build $BUILD -chunksize 16 -diskthroughput $DT
cd /tmp
zip `basename log*.csv | cut -d"." -f1` log*.csv
rm log*.csv
cd -
sleep 2
go run cmd/ck.go -ttr 10ms -m 8 -gateways 90 -servers 90 -build $BUILD -chunksize 16 -diskthroughput $DT
cd /tmp
zip `basename log*.csv | cut -d"." -f1` log*.csv
rm log*.csv
cd -
# 90x90 1M
sleep 2
go run cmd/ck.go -ttr 40ms -m 5 -gateways 90 -servers 90 -build $BUILD -chunksize 1024 -diskthroughput $DT
cd /tmp
zip `basename log*.csv | cut -d"." -f1` log*.csv
rm log*.csv
cd -
sleep 2
go run cmd/ck.go -ttr 40ms -m 6 -gateways 90 -servers 90 -build $BUILD -chunksize 1024 -diskthroughput $DT
cd /tmp
zip `basename log*.csv | cut -d"." -f1` log*.csv
rm log*.csv
cd -
sleep 2
go run cmd/ck.go -ttr 40ms -m 7 -gateways 90 -servers 90 -build $BUILD -chunksize 1024 -diskthroughput $DT
cd /tmp
zip `basename log*.csv | cut -d"." -f1` log*.csv
rm log*.csv
cd -
sleep 2
go run cmd/ck.go -ttr 40ms -m 8 -gateways 90 -servers 90 -build $BUILD -chunksize 1024 -diskthroughput $DT
cd /tmp
zip `basename log*.csv | cut -d"." -f1` log*.csv
rm log*.csv
cd -
# 30x90
sleep 2
go run cmd/ck.go -ttr 10ms -m 5 -gateways 30 -servers 90 -build $BUILD -diskthroughput $DT
cd /tmp
zip `basename log*.csv | cut -d"." -f1` log*.csv
rm log*.csv
cd -
sleep 2
go run cmd/ck.go -ttr 10ms -m 6 -gateways 30 -servers 90 -build $BUILD -diskthroughput $DT
cd /tmp
zip `basename log*.csv | cut -d"." -f1` log*.csv
rm log*.csv
cd -
sleep 2
go run cmd/ck.go -ttr 10ms -m 7 -gateways 30 -servers 90 -build $BUILD -diskthroughput $DT
cd /tmp
zip `basename log*.csv | cut -d"." -f1` log*.csv
rm log*.csv
cd -
sleep 2
go run cmd/ck.go -ttr 10ms -m 8 -gateways 30 -servers 90 -build $BUILD -diskthroughput $DT
cd /tmp
zip `basename log*.csv | cut -d"." -f1` log*.csv
rm log*.csv
cd -
# 30x90 16K
sleep 2
go run cmd/ck.go -ttr 40ms -m 5 -gateways 30 -servers 90 -build $BUILD -chunksize 16 -diskthroughput $DT
cd /tmp
zip `basename log*.csv | cut -d"." -f1` log*.csv
rm log*.csv
cd -
sleep 2
go run cmd/ck.go -ttr 40ms -m 6 -gateways 30 -servers 90 -build $BUILD -chunksize 16 -diskthroughput $DT
cd /tmp
zip `basename log*.csv | cut -d"." -f1` log*.csv
rm log*.csv
cd -
sleep 2
go run cmd/ck.go -ttr 40ms -m 7 -gateways 30 -servers 90 -build $BUILD -chunksize 16 -diskthroughput $DT
cd /tmp
zip `basename log*.csv | cut -d"." -f1` log*.csv
rm log*.csv
cd -
sleep 2
go run cmd/ck.go -ttr 40ms -m 8 -gateways 30 -servers 90 -build $BUILD -chunksize 16 -diskthroughput $DT
cd /tmp
zip `basename log*.csv | cut -d"." -f1` log*.csv
rm log*.csv
cd -
# 30x90 1M
sleep 2
go run cmd/ck.go -ttr 40ms -m 5 -gateways 30 -servers 90 -build $BUILD -chunksize 1024 -diskthroughput $DT
cd /tmp
zip `basename log*.csv | cut -d"." -f1` log*.csv
rm log*.csv
cd -
sleep 2
go run cmd/ck.go -ttr 40ms -m 6 -gateways 30 -servers 90 -build $BUILD -chunksize 1024 -diskthroughput $DT
cd /tmp
zip `basename log*.csv | cut -d"." -f1` log*.csv
rm log*.csv
cd -
sleep 2
go run cmd/ck.go -ttr 40ms -m 7 -gateways 30 -servers 90 -build $BUILD -chunksize 1024 -diskthroughput $DT
cd /tmp
zip `basename log*.csv | cut -d"." -f1` log*.csv
rm log*.csv
cd -
sleep 2
go run cmd/ck.go -ttr 40ms -m 8 -gateways 30 -servers 90 -build $BUILD -chunksize 1024 -diskthroughput $DT
cd /tmp
zip `basename log*.csv | cut -d"." -f1` log*.csv
rm log*.csv
cd -
#
