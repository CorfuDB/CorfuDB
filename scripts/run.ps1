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

# parse XML configuration
# 
[xml]$CONFIG = gc 0.aux

# sequencer 
# ################################################################################################################
$t = $CONFIG.systemview.CONFIGURATION.tokenserver
write-host tokenserver is $t
($rem, $port) = $t.split(":")
$sjar=".\corfu-sequencer.jar"
$smainclass = "com.microsoft.corfu.sequencer.CorfuSequencerImpl"

write-host tokenserver on machine $rem port $port

# push sequencer-jar to remote
#
$j = $binDir + $sjar
if ($pushflag) { 
	xcopy $j \\$rem\c$\users\$uid\corfu-bin /Y /D 
	xcopy $configFileName  \\$rem\c$\users\$uid\corfu-bin /Y /D
}
 
# start sequencer on remote
#
icm $rem -ScriptBlock { param($sjar, $smainclass, $uid
		)
		cd c:\users\$uid\corfu-bin; 
		java -classpath $sjar $smainclass
	} -ArgumentList @($sjar, $smainclass, $uid) -asjob

# unit-tests
# ################################################################################################################

# parse XML configuration
# 
[xml]$CL = gc "CLIENTS.xml"

$tstjar=".\corfu-sequencertester.jar"
$tstmainclass = "com.microsoft.corfu.unittests.CorfuSequencerTester"
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
		java -classpath $tstjar $tstmainclass $tstargs
	} -ArgumentList @($tstjar, $tstmainclass, $tstargs, $uid) -asjob
}


# sunits
# #####################################################################################################################
# $t = $xvar.systemview.CONFIGURATION.sunit
#($rem, $port) = $t.split(":")
# icm $rem { cd $home\corfu-bin; java -classpath corfu-jar com.microsoft.corfu.sequencer.CorfuStandalondServerWrapper }