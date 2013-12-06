function doicm {
	icm $args[0] -Scriptblock $args[1] -argumentList $args[2..($args.length-1)] -asjob
}


 
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

$tstjar=".\corfu-bulk.jar"
$tstmainclass = "com.microsoft.corfu.unittests.CorfuBulkdataTester"
$wthreads=1
$rthreads=1
$nrepeat=10000
$entsize = 128  * 100 
$printfreq = 500

[scriptblock]$sb = { $a = $args[0]; cd c:\users\$a\corfu-bin; java $args[1..($args.length-1)] }
 
# start client tester on all remote clients
#
$clnodes = $CL.testing.CLIENTS.CLIENT
$clnodes | %{ 
	$n = $_.nodeaddress
	write-host client on machine $n

	# push jarfile to remote
	#
	$j = $binDir + $tstjar
	if ($pushflag) { 
		xcopy $j \\$n\c$\users\$uid\corfu-bin /Y /D 
		xcopy $configFileName  \\$n\c$\users\$uid\corfu-bin /Y /D
	}

	doicm $n $sb $uid -classpath $tstjar $tstmainclass -rthreads $rthreads -wthreads $wthreads -repeat $nrepeat -size $entsize -printfreq $printfreq
}

