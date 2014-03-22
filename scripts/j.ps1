$sp = $MyInvocation.MyCommand.Definition
write-host $sp

split-path -parent $sp | ls
