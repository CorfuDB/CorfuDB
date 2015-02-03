testtype=0
echo $testtype
echo $#
if [ $# -gt 0 ] 
then
	testtype=$1
fi
echo $testtype
java -Dorg.slf4j.simpleLogger.defaultLogLevel=debug -Dorg.slf4j.simpleLogger.showDateTime=true -cp target/corfudb-0.1-SNAPSHOT-shaded.jar org.corfudb.runtime.CorfuDBTester $testtype http://localhost:8002/corfu 2>&1 | tee dummy.out
