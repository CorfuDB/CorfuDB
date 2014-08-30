function compile { push-location c:\users\dalia\SkyDrive\Projects\CORFU\CORFULIB ; mvn install; pop-location }

if (-not ($env:CLASSPATH -match "corfu-lib"))
{
	$scriptPath = split-path -parent $MyInvocation.MyCommand.Definition | split-path -parent

	$corfujar = ls $scriptPath"\CORFULIB\target\corfu-lib-*-SNAPSHOT-shaded.jar"	
	c:\users\dalia\Documents\Windowspowershell\addtoclasspath $corfujar
	write-host $corfujar
# 	$corfujar | get-member
}

function corfurun { 
        $mainclass = "com.microsoft.corfu." + $args[0]
        java $mainclass $args[1..($args.length-1)]
}

function corfubgrun {
        $mainclass = "com.microsoft.corfu." + $args[0]
	$a = $pwd
	[scriptblock]$sb = { cd $a; java $mainclass $args }
	start-job -scriptblock $sb -argumentlist $mainclass,$args[1..($args.length-1)]
}


