
# The Java implementation to use. This is required.
#export JAVA_HOME=

# Any JVM options to pass.
#export YCSB_OPTS="-Djava.compiler=NONE"

# YCSB client heap size.
#export YCSB_HEAP_SIZE=500

this=`dirname "$0"`
this=`cd "$this"; pwd`

while [ -h "$this" ]; do
  ls=`ls -ld "$this"`
  link=`expr "$ls" : '.*-> \(.*\)$'`
  if expr "$link" : '.*/.*' > /dev/null; then
    this="$link"
  else
    this=`dirname "$this"`/"$link"
  fi
done

bin=`dirname "$this"`
script=`basename "$this"`
bin=`cd "$bin"; pwd`
this="$bin/$script"

# the root of the YCSB installation
export YCSB_HOME=`dirname "$this"`

cygwin=false
case "`uname`" in
CYGWIN*) cygwin=true;;
esac

JAVA=""
if [ "$JAVA_HOME" != "" ]; then
    if $cygwin; then
	JAVA_HOME=`cygpath "$JAVA_HOME"`
    fi
    JAVA=$JAVA_HOME/bin/java
else
  echo "JAVA_HOME must be set."
  exit 1
fi

JAVA_HEAP_MAX=-Xmx500m
# check envvars which might override default args
if [ "$YCSB_HEAP_SIZE" != "" ]; then
  JAVA_HEAP_MAX="-Xmx""$YCSB_HEAP_SIZE""m"
fi

# Set the classpath.
if [ "$CLASSPATH" != "" ]; then
  CLASSPATH=${CLASSPATH}:$JAVA_HOME/lib/tools.jar
else
  CLASSPATH="$JAVA_HOME/lib/tools.jar"
fi

# Set CORFUDB_HOME
if [ "$CORFUDB_HOME" == "" ]; then
    CORFUDB_HOME=../CorfuDB
    echo "CORFUDB_HOME=$CORFUDB_HOME"
fi

for f in $CORFUDB_HOME/target/*shaded.jar; do
  echo $f
  CLASSPATH=${CLASSPATH}:$f
done

# so that filenames w/ spaces are handled correctly in loops below
IFS=

for f in $YCSB_HOME/core/lib/*.jar; do
  CLASSPATH=${CLASSPATH}:$f
done

# restore ordinary behavior
unset IFS

# cygwin path translation
if $cygwin; then
  export CLASSPATH=`cygpath -p -w $CLASSPATH`
  export YCSB_HOME=`cygpath -w "$YCSB_HOME"`
fi

#echo "Setup class path = $CLASSPATH with options $JAVA_HEAP_MAX $YCSB_OPTS"
exec java -cp "$CLASSPATH" com.yahoo.ycsb.Client -db org.corfudb.runtime.YCSBClient -P workloads/workloada -p corfudb.masternode=http://localhost:8002/corfudb -t

