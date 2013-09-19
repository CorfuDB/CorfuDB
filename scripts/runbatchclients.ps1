# clean up previous jobs
write-host killing all previous jobs ...
stop-job *
rjb *

# distribution parameters
#
$scriptPath = split-path -parent $MyInvocation.MyCommand.Definition | split-path -parent
$configFileName = "0.aux"
$binDir = $scriptPath + "\bin\java\"
$uid = $env:username

# parse commandline arguments
#
$pushflag = $false
if ($args.count -gt 0 -and $args[0] -eq "-push") { $pushflag = $true }
if ($pushflag) { write-host push updates to destinations } 
	else   { write-host do not push updates to destinations }

# unit-tests
# ################################################################################################################

# parse XML configuration
# 
[xml]$CL = gc "CLIENTS.xml"

$tstjar=".\corfu-seqbatchtester.jar"
$tstmainclass = "com.microsoft.corfu.unittests.CorfuSeqBatchTester"
$tstargs = -threads 64 -repeat 100000

$clnodes = $CL.testing.CLIENTS.CLIENT

 
# start sequencertster on all remote clients
#
$clnodes | %{ 
	$n = $_.nodeaddress
	write-host client on machine $n

	# push sequencertester-jar to remote
	#
	$j = $binDir + $tstjar
	if ($pushflag) { 
		xcopy $j \\$n\c$\users\$uid\corfu-bin /Y /D 
		xcopy $configFileName  \\$n\c$\users\$uid\corfu-bin /Y /D
	}

	icm $n -ScriptBlock { param($tstjar, $tstmainclass, $tstargs, $uid) 
		cd c:\users\$uid\corfu-bin;
		java -classpath $tstjar $tstmainclass -consumerthreads 64 -producerthreads 4 -repeat 100000
	} -ArgumentList @($tstjar, $tstmainclass, $tstargs, $uid) -asjob
}

