Param(
    $rammode,
    $recover
)
if ($rammode -ne $null) { write-host ram mode }
if ($recover -ne $null) { write-host recovery mode }


##################################################################################################

# clean up previous jobs
write-host killing all previous jobs ...
stop-job *
rjb *

# distribution parameters
#
$scriptPath = split-path -parent $MyInvocation.MyCommand.Definition 
$configFileName = $scriptPath + "\corfu.xml"
$package = "com.microsoft.corfu."
[scriptblock]$sb = { cd $scriptPath; java $args }

# parse XML configuration
# 
[xml]$CONFIG = gc $configFileName

# sequencer 
# ################################################################################################################
$t = $CONFIG.CONFIGURATION.sequencer
# write-host tokenserver is $t
($rem, $port) = $t.split(":")
$smainclass = $package+"sequencer.SequencerDriver"
 
# start sequencer 
#
start-job -scriptblock $sb -argumentlist $smainclass,$port 

# sunit servers 
# ################################################################################################################
$sunits = $CONFIG.CONFIGURATION.SEGMENT.GROUP.NODE
$sunits | %{ $ind=0} {
	$n = $_.nodeaddress

	($rem, $port) = $n.split(":")
	$smainclass = $package+"loggingunit.LogUnitDriver"

        $drivename = "c:\temp\foo{0}.{1}.txt" -f $ind,0
	# start unit server 
	#
	if ($recover -ne $null) {
		start-job -scriptblock $sb -argumentlist $smainclass,-port,$port,-drivename,$drivename,-recover
	} elseif ($rammode -ne $null) {
		start-job -scriptblock $sb -argumentlist $smainclass,-port,$port,-rammode
	} else {
		start-job -scriptblock $sb -argumentlist $smainclass,-port,$port,-drivename,$drivename
	}

	$ind++
}
