#!/bin/bash
set -x
BUILD=`git rev-parse --short HEAD`
# 90x90 16K
go run cmd/ck.go -ttr 10ms -m 5 -servers 90 -gateways 90 -build $BUILD -chunksize 16
cd /tmp
zip `basename log*.csv | cut -d"." -f1` log*.csv
rm log*.csv
cd -
sleep 2
go run cmd/ck.go -ttr 10ms -m 6 -servers 90 -gateways 90 -build $BUILD -chunksize 16
cd /tmp
zip `basename log*.csv | cut -d"." -f1` log*.csv
rm log*.csv
cd -
sleep 2
go run cmd/ck.go -ttr 10ms -m 7 -servers 90 -gateways 90 -build $BUILD -chunksize 16
cd /tmp
zip `basename log*.csv | cut -d"." -f1` log*.csv
rm log*.csv
cd -
sleep 2
go run cmd/ck.go -ttr 10ms -m 8 -servers 90 -gateways 90 -build $BUILD -chunksize 16
cd /tmp
zip `basename log*.csv | cut -d"." -f1` log*.csv
rm log*.csv
cd -
# 90x90 1M
sleep 2
go run cmd/ck.go -ttr 30ms -m 5 -servers 90 -gateways 90 -build $BUILD -chunksize 1024
cd /tmp
zip `basename log*.csv | cut -d"." -f1` log*.csv
rm log*.csv
cd -
sleep 2
go run cmd/ck.go -ttr 30ms -m 6 -servers 90 -gateways 90 -build $BUILD -chunksize 1024
cd /tmp
zip `basename log*.csv | cut -d"." -f1` log*.csv
rm log*.csv
cd -
sleep 2
go run cmd/ck.go -ttr 30ms -m 7 -servers 90 -gateways 90 -build $BUILD -chunksize 1024
cd /tmp
zip `basename log*.csv | cut -d"." -f1` log*.csv
rm log*.csv
cd -
sleep 2
go run cmd/ck.go -ttr 30ms -m 8 -servers 90 -gateways 90 -build $BUILD -chunksize 1024
cd /tmp
zip `basename log*.csv | cut -d"." -f1` log*.csv
rm log*.csv
cd -
# 90x90 
sleep 2
go run cmd/ck.go -ttr 10ms -m 5 -servers 90 -gateways 90 -build $BUILD
cd /tmp
zip `basename log*.csv | cut -d"." -f1` log*.csv
rm log*.csv
cd -
sleep 2
go run cmd/ck.go -ttr 10ms -m 6 -servers 90 -gateways 90 -build $BUILD
cd /tmp
zip `basename log*.csv | cut -d"." -f1` log*.csv
rm log*.csv
cd -
sleep 2
go run cmd/ck.go -ttr 10ms -m 7 -servers 90 -gateways 90 -build $BUILD
cd /tmp
zip `basename log*.csv | cut -d"." -f1` log*.csv
rm log*.csv
cd -
sleep 2
go run cmd/ck.go -ttr 10ms -m 8 -servers 90 -gateways 90 -build $BUILD
cd /tmp
zip `basename log*.csv | cut -d"." -f1` log*.csv
rm log*.csv
cd -
# 30x90 
sleep 2
go run cmd/ck.go -ttr 10ms -m 5 -servers 90 -gateways 30 -build $BUILD
cd /tmp
zip `basename log*.csv | cut -d"." -f1` log*.csv
rm log*.csv
cd -
sleep 2
go run cmd/ck.go -ttr 10ms -m 6 -servers 90 -gateways 30 -build $BUILD
cd /tmp
zip `basename log*.csv | cut -d"." -f1` log*.csv
rm log*.csv
cd -
sleep 2
go run cmd/ck.go -ttr 10ms -m 7 -servers 90 -gateways 30 -build $BUILD
cd /tmp
zip `basename log*.csv | cut -d"." -f1` log*.csv
rm log*.csv
cd -
sleep 2
go run cmd/ck.go -ttr 10ms -m 8 -servers 90 -gateways 30 -build $BUILD
cd /tmp
zip `basename log*.csv | cut -d"." -f1` log*.csv
rm log*.csv
cd -
# 30x90 1M
sleep 2
go run cmd/ck.go -ttr 30ms -m 5 -servers 90 -gateways 30 -build $BUILD -chunksize 1024
cd /tmp
zip `basename log*.csv | cut -d"." -f1` log*.csv
rm log*.csv
cd -
sleep 2
go run cmd/ck.go -ttr 30ms -m 6 -servers 90 -gateways 30 -build $BUILD -chunksize 1024
cd /tmp
zip `basename log*.csv | cut -d"." -f1` log*.csv
rm log*.csv
cd -
sleep 2
go run cmd/ck.go -ttr 30ms -m 7 -servers 90 -gateways 30 -build $BUILD -chunksize 1024
cd /tmp
zip `basename log*.csv | cut -d"." -f1` log*.csv
rm log*.csv
cd -
sleep 2
go run cmd/ck.go -ttr 30ms -m 8 -servers 90 -gateways 30 -build $BUILD -chunksize 1024
cd /tmp
zip `basename log*.csv | cut -d"." -f1` log*.csv
rm log*.csv
cd -
# 30x90 16K
sleep 2
go run cmd/ck.go -ttr 30ms -m 5 -servers 90 -gateways 30 -build $BUILD -chunksize 16
cd /tmp
zip `basename log*.csv | cut -d"." -f1` log*.csv
rm log*.csv
cd -
sleep 2
go run cmd/ck.go -ttr 30ms -m 6 -servers 90 -gateways 30 -build $BUILD -chunksize 16
cd /tmp
zip `basename log*.csv | cut -d"." -f1` log*.csv
rm log*.csv
cd -
sleep 2
go run cmd/ck.go -ttr 30ms -m 7 -servers 90 -gateways 30 -build $BUILD -chunksize 16
cd /tmp
zip `basename log*.csv | cut -d"." -f1` log*.csv
rm log*.csv
cd -
sleep 2
go run cmd/ck.go -ttr 30ms -m 8 -servers 90 -gateways 30 -build $BUILD -chunksize 16
cd /tmp
zip `basename log*.csv | cut -d"." -f1` log*.csv
rm log*.csv
cd -
#
