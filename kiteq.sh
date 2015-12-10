# ./kiteq -bind=:23800 -pport=23801 -db="file://.?cap=10000000&checkSeconds=10" -topics=trade  -zkhost=localhost:2181 -logxml=./log/log_test.xml -fly=true
#./kiteq -bind=:23800 -pport=23801 -db="mock://.?cap=10000000&checkSeconds=10" -topics=trade  -zkhost=localhost:2181 -logxml=./log/log_test.xml
 ./kiteq -bind=:23800 -pport=23801 -db="mysql://localhost:3306?db=kite&username=root&password=root&batchUpdateSize=2000&batchDelSize=10000&flushSeconds=1" -topics=trade  -zkhost=localhost:2181 -logxml=./log/log_test.xml

