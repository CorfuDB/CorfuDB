#!/usr/bin/env python3

"""
USAGE:
1. To trigger compactor instantly without trimming at the end of the cycle
/usr/share/corfu/scripts/compactor_runner.py --port <port> --compactorConfig /usr/share/corfu/conf/corfu-compactor-config.yml --instantTriggerCompaction=True
2. To trigger compactor instantly with trimming at the end of the cycle
/usr/share/corfu/scripts/compactor_runner.py --port <port> --compactorConfig /usr/share/corfu/conf/corfu-compactor-config.yml --instantTriggerCompaction=True --trimAfterCheckpoint=True
3. To freeze compactor (This stops running compactor for 2 hrs from the time of freezing)
/usr/share/corfu/scripts/compactor_runner.py --port <port> --compactorConfig /usr/share/corfu/conf/corfu-compactor-config.yml --freezeCompaction=True
4. To unfreeze compactor
/usr/share/corfu/scripts/compactor_runner.py --port <port> --compactorConfig /usr/share/corfu/conf/corfu-compactor-config.yml --unfreezeCompaction=True
5. To disable compactor (This stops running compactor until it is enabled again)
/usr/share/corfu/scripts/compactor_runner.py --port <port> --compactorConfig /usr/share/corfu/conf/corfu-compactor-config.yml --disableCompaction=True
6. To enable compactor
/usr/share/corfu/scripts/compactor_runner.py --port <port> --compactorConfig /usr/share/corfu/conf/corfu-compactor-config.yml --enableCompaction=True
"""

from __future__ import absolute_import, print_function
from argparse import ArgumentParser
import glob
import logging
import netifaces
import os
import os.path
from subprocess import check_call, check_output, STDOUT
import time
import yaml

COMPACTOR_CONTROLS_CLASS_NAME = "org.corfudb.compactor.CompactorController"
COMPACTOR_CHECKPOINTER_CLASS_NAME = "org.corfudb.compactor.CompactorCheckpointer"
MAX_CACHE_ENTRIES = 0
COMPACTOR_BULK_READ_SIZE = 20
COMPACTOR_JVM_XMX = 1024
FORCE_DISABLE_CHECKPOINTING = "FORCE_DISABLE_CHECKPOINTING"
POD_NAME = "POD_NAME"
POD_NAMESPACE = "POD_NAMESPACE"
IN_MEM_MIN_XMX = 512
DISK_BACKED_MIN_XMX = 150
DISK_BACKED_MAX_XMX = 1500

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

class Config(object):
    def __init__(self):
        """
        Initialize an empty configuration.
        Fields are public to keep it simple.
        """
        self.network_interface = None
        self.network_interface_version = None
        self.hostname = None
        self.corfu_port = None
        self.configPath = None
        self.startCheckpointing = None
        self.instantTriggerCompaction = None
        self.trim = None
        self.freezeCompaction = None
        self.unfreezeCompaction = None
        self.disableCompaction = None
        self.enableCompaction = None
        self.xmx_perc = 0.65 # arbitrary

class CommandBuilder(object):
    def __init__(self, config):
        self._config = config

    def derive_xmx_value(self, disk_backed, use_std_disk_backed_config, compactor_config):
        try:
            mem_bytes = os.sysconf('SC_PAGE_SIZE') * os.sysconf('SC_PHYS_PAGES')
            mem_mb = int(mem_bytes / (1024. ** 2))
            if disk_backed is False:
                xmx = max(IN_MEM_MIN_XMX, int(mem_mb * 0.04))
            elif use_std_disk_backed_config is True:
                xmx = max(DISK_BACKED_MIN_XMX, int(mem_mb * float(self._config.xmx_perc)/100))
            else:
                xmx = min(DISK_BACKED_MAX_XMX, int(mem_mb * 0.025))
            return xmx
        except Exception as ex:
            return COMPACTOR_JVM_XMX

    def append_compactor_config(self, compactor_config):
        GCParameters = compactor_config["GCParameters"]
        cmd = []
        if "UseConcMarkSweepGC" in GCParameters and GCParameters["UseConcMarkSweepGC"] is True:
            cmd.append("-XX:+UseConcMarkSweepGC")
        if "UseG1GC" in GCParameters and GCParameters["UseG1GC"] is True:
            cmd.append("-XX:+UseG1GC")
        if "PrintGCDetails" in GCParameters and GCParameters['PrintGCDetails'] is True:
            gc_str = '-Xlog:safepoint,gc*=debug:file=' + GCParameters["Logpath"]
            if 'PrintGCTimeStamps' in GCParameters and GCParameters['PrintGCTimeStamps'] is True:
                gc_str += ':time,uptime,level'
            if 'UseGCLogFileRotation' in GCParameters and GCParameters['UseGCLogFileRotation'] is True:
                gc_str += ':filecount=' + str(GCParameters['NumberOfGCLogFiles']) + ',filesize=' + GCParameters['GCLogFileSize']
            cmd.append(gc_str)

        ConfigFiles = compactor_config["ConfigFiles"]
        cmd.append("-Djava.io.tmpdir=" + ConfigFiles["TempDir"])
        cmd.append("-Dlogback.configurationFile=" + ConfigFiles["CompactorLogbackPath"])
        cmd.append("-XX:HeapDumpPath=" + ConfigFiles["HeapDumpPath"])
        cmd.append("-XX:OnOutOfMemoryError=\"gzip -f " + ConfigFiles["HeapDumpPath"] + "\"")

        corfudb_tool_shaded_jar = ""
        for name in glob.glob(ConfigFiles["ClassPath"]):
            corfudb_tool_shaded_jar = name
            break
        if corfudb_tool_shaded_jar == "":
            print("Can't find the corfudb-tools-shaded*jar")
            quit()

        cmd.append("-cp")
        cmd.append(corfudb_tool_shaded_jar)

        return " ".join(cmd)

    def get_corfu_compactor_cmd(self, compactor_config, class_to_invoke):
        disk_backed = False
        use_std_disk_backed_config = False
        if "DiskBacked" in compactor_config["MemoryOptions"] and compactor_config["MemoryOptions"]['DiskBacked'] is True:
            disk_backed = True
        if "UseStdDiskBackedConfig" in compactor_config["MemoryOptions"] and compactor_config["MemoryOptions"]['UseStdDiskBackedConfig'] is True:
            use_std_disk_backed_config = True
        xmx = self.derive_xmx_value(disk_backed, use_std_disk_backed_config, compactor_config)

        cmd = []
        cmd.append("MALLOC_TRIM_THRESHOLD_=1310720")
        cmd.append("java")
        cmd.append("-XX:+UseStringDeduplication")
        cmd.append("-XX:+HeapDumpOnOutOfMemoryError")
        cmd.append("-XX:+CrashOnOutOfMemoryError")
        cmd.append("-XX:+AlwaysPreTouch")
        if 'RootDir' in compactor_config['CorfuPaths']:
            cmd.append("-XX:ErrorFile=" + compactor_config['CorfuPaths']['RootDir'] + "hs_err_pid%p.log")
        cmd.append("-Xms" + str(xmx) + "m")
        cmd.append("-Xmx" + str(xmx) + "m")
        cmd.append("-Djdk.nio.maxCachedBufferSize=1048576")
        cmd.append("-Dio.netty.recycler.maxCapacityPerThread=0")
        cmd.append(self.append_compactor_config(compactor_config))
        cmd.append(class_to_invoke)
        cmd.append("--hostname=" + self._config.hostname)
        cmd.append("--port=" + self._config.corfu_port)
        cmd.append("--tlsEnabled=true")
        cmd.append("--bulkReadSize=" + str(COMPACTOR_BULK_READ_SIZE))
        cmd.append("--maxCacheEntries=" + str(MAX_CACHE_ENTRIES))

        Security = compactor_config["Security"]
        cmd.append("--keystore=" + Security["Keystore"])
        cmd.append("--ks_password=" + Security["KsPassword"])
        cmd.append("--truststore=" + Security["Truststore"])
        cmd.append("--truststore_password=" + Security["TruststorePassword"])

        if disk_backed is True:
            cmd.append("--persistedCacheRoot=" + compactor_config["MemoryOptions"]["DiskPath"])

        if not self._config.startCheckpointing:
            if self._config.instantTriggerCompaction:
                cmd.append("--instantTriggerCompaction=true")
            if self._config.trim:
                cmd.append("--trim=true")
            if self._config.freezeCompaction:
                cmd.append("--freezeCompaction=true")
            if self._config.unfreezeCompaction:
                cmd.append("--unfreezeCompaction=true")
            if self._config.disableCompaction:
                cmd.append("--disableCompaction=true")
            if self._config.enableCompaction:
                cmd.append("--enableCompaction=true")

        return " ".join(cmd)


class CompactorRunner(object):
    def __init__(self, args):
        """
        Initialize components and read user provided configuration.
        """
        self._config = self._complete_config(args)
        self._command_builder = CommandBuilder(self._config)

    def run(self):
        """
        Run compactor.
        """
        self._print_and_log("Invoked compactor_runner...")
        if self._config.freezeCompaction and self._config.unfreezeCompaction:
            self._print_and_log("ERROR: Both freeze and unfreeze compaction parameters cannot be passed together")
            return
        if self._config.enableCompaction and self._config.disableCompaction:
            self._print_and_log("ERROR: Both enable and disable compaction parameters cannot be passed together")
            return
        self._run_corfu_compactor()

    def _print_and_log(self, msg):
        logger.info(msg)
        print(msg)

    # Disk space: max 50MB. keep 1000 files.
    # Mem space: max 50MB, most of time 50KB. CORFU_GC_MAX_INDEX 1000 files, each file is 50KB.
    def _rsync_log(self, src_file_prefix, dst_dir):
        self._print_and_log("start copying jvm gc files from  " + src_file_prefix + " to " + dst_dir)
        src_dir = os.path.dirname(src_file_prefix)
        if not os.path.isdir(src_dir):
            self._print_and_log("ERROR nonexist dir " + src_dir)

        if not os.path.isdir(dst_dir):
            check_output("mkdir " + dst_dir, shell=True)
            self._print_and_log("create dst " + dst_dir)

        flist = glob.glob(src_file_prefix + "*")
        for file in flist:
            try:
                # Java 11 log rotation naming scheme:
                # 1. The active GC log file has a ".log" extension
                # 2. Rotated GC files have a ".log.[0-9]" extension
                if file.split(".")[-1] != 'log':
                    cmd = "cp --preserve " + file + " " + dst_dir
                    output = check_output(cmd, shell=True)
                    os.remove(file)
                    msg = "CMD: " + cmd
                    self._print_and_log(msg)
            except Exception as ex:
                self._print_and_log("Failed to copy log file: " + file + ", error: " + str(ex))
        self._print_and_log("Done copying jvm gc files from  " + src_file_prefix + " to " + dst_dir)

    def _run_corfu_compactor(self):
        """
        Run the corfu compactor. It will first trim and then immediately checkpoint all corfu maps.
        Now it is mainly for running it periodically.
        Note: need to ensure the 15min gap between two runs of this tool to avoid trim exception.
        """
        with open(self._config.configPath, "r") as config:
            compactor_config = yaml.load(config, yaml.FullLoader)
        corfu_paths = compactor_config["CorfuPaths"]
        log_replicator_paths = None
        if "LogReplicatorPaths" in compactor_config:
            log_replicator_paths = compactor_config["LogReplicatorPaths"]

        logging.basicConfig(filename=corfu_paths["CompactorLogfile"],
                    format='%(asctime)s.%(msecs)03dZ %(levelname)5s Runner - %(message)s',
                    datefmt='%Y-%m-%dT%H:%M:%S')
        # Copy Corfu and LogReplicator(if running) jvm gc log files to disk
        try:
            self._rsync_log(corfu_paths["CorfuMemLogPrefix"], corfu_paths["CorfuDiskLogDir"])
            if log_replicator_paths is not None and os.path.isdir(log_replicator_paths["LogReplicatorDiskLogDir"]):
                self._rsync_log(log_replicator_paths["LogReplicatorMemLogPrefix"],
                                log_replicator_paths["LogReplicatorDiskLogDir"])
        except Exception as ex:
            self._print_and_log("Failed to run rsync_log " + " error: " + str(ex))

        self._configure_server_address(compactor_config)
        self._print_and_log("_run_corfu_compactor: Configured compacter hostname as "
                            + self._config.hostname
                            + ", port as "
                            + self._config.corfu_port)

        if self._config.startCheckpointing:
            grep_running_tool = "ps aux | grep 'python3 /usr/share/corfu/scripts/compactor_runner.py' | grep 'startCheckpointing' | grep -v 'grep' | grep " + self._config.corfu_port

            try:
                grep_tool_result = check_output(grep_running_tool, shell=True).decode()
            except Exception as ex:
                self._print_and_log("Failed to run grep command: " + grep_running_tool + ", error: " + str(ex))
                raise

            self._print_and_log("Result for " + grep_running_tool + ":\n" + grep_tool_result)

            # At least one (this current invocation) should be running.
            if grep_tool_result.count("\n") < 2:
                self._print_and_log("No other compactor tool is running.")
            else:
                # Compactor is already running (more than one result for the grepping python command), just exit.
                self._print_and_log("Other compactor tool is already running.")
                return

        # set environment param MALLOC_TRIM_THRESHOLD_=1310720
        exp_command = "export MALLOC_TRIM_THRESHOLD_=1310720"
        check_exp_output = check_output(exp_command, shell=True).decode()
        self._print_and_log("Result for " + exp_command + ":\n" + check_exp_output)

        try:
            # call compactor
            self._print_and_log("============= COMPACTOR ==============")
            class_to_invoke = COMPACTOR_CONTROLS_CLASS_NAME
            if self._config.startCheckpointing:
                # If the env var FORCE_DISABLE_CHECKPOINTING is set to True, do not run checkpointing.
                # This is used by Upgrade dry-run tool to disable compaction on one node.
                force_disable_checkpointing = (os.environ.get(FORCE_DISABLE_CHECKPOINTING, "False") == "True")
                if not force_disable_checkpointing:
                    class_to_invoke = COMPACTOR_CHECKPOINTER_CLASS_NAME
                    self._print_and_log("Invoke CompactorCheckpointer...")
                else:
                    self._print_and_log("Force disabled checkpointing, hence exiting")
                    return
            cmd = self._command_builder.get_corfu_compactor_cmd(compactor_config, class_to_invoke)
            self._print_and_log("Start compacting. Command %s" % cmd)
            with open(corfu_paths["CompactorLogfile"], 'a') as f:
                check_call(cmd, stdout=f, stderr=f, shell=True)
            self._print_and_log("Finished running corfu compactor tool.")

        except Exception as ex:
            self._print_and_log("Failed to run compactor tool: %s" % str(ex))
            time.sleep(10)

    def _complete_config(self, args):
        """
        Setup the wizard configuration.
        Interactively asking for input if any required parameter is missing.
        Return a Config object.
        """
        config = Config()
        config.configPath = args.compactorConfig
        if 'hostname' in args and args.hostname:
            config.hostname = args.hostname
        if 'port' in args and args.port:
            config.corfu_port = args.port
        if 'network_interface' in args and args.network_interface:
            config.network_interface = args.network_interface
        if 'network_interface_version' in args and args.network_interface_version:
            config.network_interface_version = args.network_interface_version
        if 'instantTriggerCompaction' in args:
            config.instantTriggerCompaction = args.instantTriggerCompaction
        if 'trimAfterCheckpoint' in args:
            config.trim = args.trimAfterCheckpoint
        if 'freezeCompaction' in args:
            config.freezeCompaction = args.freezeCompaction
        if 'unfreezeCompaction' in args:
            config.unfreezeCompaction = args.unfreezeCompaction
        if 'disableCompaction' in args:
            config.disableCompaction = args.disableCompaction
        if 'enableCompaction' in args:
            config.enableCompaction = args.enableCompaction
        if 'startCheckpointing' in args:
            config.startCheckpointing = args.startCheckpointing
        return config

    def _configure_server_address(self, compactor_config):
        """
        Configure Server hostname and port from args and config file.
        This method invokes resolve ip method to configure from network interfaces
        when the hostname is not present in args or config file.

        :param compactor_config: values parsed from config file
        :return:
        """
        server_address = None
        if 'ServerAddress' in compactor_config:
            server_address = compactor_config["ServerAddress"]
        else:
            self._print_and_log("WARNING: _configure_server_address: ServerAddress is not present in config file.")

        if not self._config.corfu_port:
            if server_address and "Port" in server_address and server_address["Port"]:
                self._config.corfu_port = str(server_address["Port"])
            else:
                # When port is not present in the server args or port param is not there in the config file
                raise RuntimeError("_configure_server_address: Port is not present in args or config file."
                                   " Please check the configurations.")

        # configuring hostname and port
        pod_name = os.environ.get(POD_NAME, "default_name")
        pod_namespace = os.environ.get(POD_NAMESPACE, "default_namespace")
        if pod_name != "default_name" and pod_namespace != "default_namespace":
            self._config.hostname = pod_name + ".corfu-headless." + pod_namespace + ".svc.cluster.local"
            return

        if not self._config.hostname:
            if self._config.network_interface and self._config.network_interface_version:
                self._print_and_log("WARNING: _configure_server_address: Hostname is not present in args."
                                    " Continuing self IP discovery with {}"
                                    " interface and {} version from args.".format(self._config.network_interface,
                                                                                  self._config.network_interface_version))
                self._config.hostname = self._resolve_ip_address(
                    network_interface=self._config.network_interface,
                    network_interface_version=self._config.network_interface_version
                )
                return
            elif server_address:
                self._print_and_log("_configure_server_address: Found the server_address from config file: {}"
                                    .format(server_address))
                if "Hostname" in server_address and server_address["Hostname"]:
                    self._config.hostname = server_address["Hostname"]
                    return
                # Do self ip discovery since hostname is not configured
                elif "NetworkInterfaceVersion" in server_address and server_address["NetworkInterfaceVersion"] \
                     and "NetworkInterface" in server_address and server_address["NetworkInterface"]:
                    self._print_and_log("WARNING: _configure_server_address: Hostname is not present in args or"
                                        " config file. Continuing self IP discovery with {} interface and {} version"
                                        " from config file.".format(server_address["NetworkInterface"],
                                                                    server_address["NetworkInterfaceVersion"]))
                    self._config.hostname = self._resolve_ip_address(
                        network_interface=server_address["NetworkInterface"],
                        network_interface_version=server_address["NetworkInterfaceVersion"]
                    )
                    return
                else:
                    raise RuntimeError("_configure_server_address: Hostname or network interface info"
                                       " is not present in args or config file. Please check the configurations.")
            else:
                # When server address param is not there in the config file or hostname is not present in the args
                raise RuntimeError("_configure_server_address: Couldn't load the server_address from args or config"
                                   " file. Please check the configurations.")


    def _resolve_ip_address(self, network_interface=None, network_interface_version=None):
        """
        If environment variable or hostname config is set, resolve using it. Else, get an
        address of from the network interfaces. IPv6 is preferred over IPv4.
        """
        network_interface_version = network_interface_version.upper()
        network_interfaces = netifaces.interfaces()
        network_interface_versions_dict = {'IPV4': netifaces.AF_INET, 'IPV6': netifaces.AF_INET6}
        generic_interfaces = ['eth0', 'en0', 'eth', 'en', 'lo']

        for iteration in range(2):
            try:
                return netifaces.ifaddresses(network_interface)[network_interface_versions_dict[network_interface_version]][0]['addr']
            except (KeyError, ValueError) as e:
                print('Unable to find valid IP in the interface %s at iteration %s, looking for other options.'
                      % (network_interface, iteration), e)

                for generic_interface in generic_interfaces:
                    for network_interface in network_interfaces:
                        if network_interface.startswith(generic_interface):
                            try:
                                return netifaces.ifaddresses(network_interface)[network_interface_versions_dict[network_interface_version]][0]['addr']
                            except (KeyError, ValueError) as e:
                                self._print_and_log('Unable to find valid IP in the interface %s at iteration %s,'
                                                    ' looking for other options.'
                                                    .format(network_interface, iteration, e))

            if iteration == 0:
                if network_interface_version == 'IPV6':
                    network_interface_version = 'IPV4'
                else:
                    network_interface_version = 'IPV6'

        raise RuntimeError("Could not find any IP addresses. "
                           "Please check the network interfaces and the program arguments.")

if __name__ == "__main__":
    arg_parser = ArgumentParser()

    arg_parser.add_argument("--network_interface", type=str,
                            help="The network interface that corfu server is listening to. "
                                 "Default value from config file is eth0.",
                            required=False)
    arg_parser.add_argument("--network_interface_version", type=str,
                            help="The network interface version that corfu server is listening to. "
                                 "Default value from config file is IPv4.",
                            required=False)
    arg_parser.add_argument("--hostname", type=str,
                            help="The corfu server hostname",
                            required=False)
    arg_parser.add_argument("--port", type=str,
                            help="The corfu server port number. "
                                 "Default value from config file is 9000.",
                            required=False)
    arg_parser.add_argument("--compactorConfig", type=str,
                            help="The file containing config for compactor",
                            default="/usr/share/corfu/conf/corfu-compactor-config.yml",
                            required=False)
    arg_parser.add_argument("--startCheckpointing", type=bool, default=False,
                            help="Start checkpointing tables if compaction cycle has started",
                            required=False)
    arg_parser.add_argument("--instantTriggerCompaction", type=bool,
                            help="To instantly trigger compaction cycle",
                            required=False)
    arg_parser.add_argument("--freezeCompaction", type=bool,
                            help="To freeze compaction",
                            required=False)
    arg_parser.add_argument("--unfreezeCompaction", type=bool,
                            help="To unfreeze compaction",
                            required=False)
    arg_parser.add_argument("--disableCompaction", type=bool,
                            help="To disable compaction",
                            required=False)
    arg_parser.add_argument("--enableCompaction", type=bool,
                            help="To enable compaction",
                            required=False)
    arg_parser.add_argument("--trimAfterCheckpoint", type=bool,
                            help="To enable trim again after checkpointing all tables",
                            required=False)
    args = arg_parser.parse_args()
    compactor_runner = CompactorRunner(args)
    compactor_runner.run()
