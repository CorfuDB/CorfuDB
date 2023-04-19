import logging
import subprocess
import sys
import time
import yaml

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

NUM_RETRIES = 120

def set_logger(config_file_path):
    with open(config_file_path, "r") as config:
        compactor_config = yaml.load(config, yaml.FullLoader)
    corfu_paths = compactor_config["CorfuPaths"]
    logging.basicConfig(filename=corfu_paths["CompactorLogfile"],
                        format='%(asctime)s.%(msecs)03dZ %(levelname)5s Runner - %(message)s',
                        datefmt='%Y-%m-%dT%H:%M:%S', force = True)

def log_info(msg):
    logger.info(msg)

def wait_for_compactor_end(port, target_cycle_count):
    for _ in range(NUM_RETRIES):
        status, cycle_count = get_compactor_status(port)
        if cycle_count >= target_cycle_count and status != "STARTED":
            log_info("status: " + status + " cycleCount: " + str(cycle_count))
            return
        else:
            time.sleep(5)

def get_compactor_status(port):
    cmd = "/opt/vmware/bin/corfu_tool_runner.py -t CompactionManagerTable -n CorfuSystem -o showTable --port " + port
    result = subprocess.check_output(cmd, shell=True).decode()
    status = "IDLE"
    cycle_count = 0
    for line in result.split("\n"):
        if "status\"" in line:
            status = line.split(":")[-1].split("\"")[1]
        if "cycleCount\"" in line:
            cycle_count = line.split(":")[-1].split("\"")[1]
            break
    return status, int(cycle_count)

def wait_and_verify_compactor_success(port, target_cycle_count):
    """
    This method waits for a compactor cycle as specified by the target_cycle_count to end
    :param port: port to which the compactor client should connect to the corfu server
    :param target_cycle_count: the number that denotes the compactor cycle
    :return:
        True if the compactor cycle completed successfully, else returns False
    """
    wait_for_compactor_end(port, target_cycle_count)
    status, cycle_count = get_compactor_status(port)
    if status == "COMPLETED":
        log_info("Compaction status for corfu at port: " + str(port) + " is COMPLETED")
        return True
    log_info("Compaction status for corfu at port: " + str(port) + " is " + status)
    return False

def invoke_compactor_runner(args, port, config_file_path, trim_after_checkpoint):
    sys.path.insert(1, '/usr/share/corfu/scripts')
    from compactor_runner import CompactorRunner
    args.port = port
    args.compactorConfig = config_file_path
    args.trimAfterCheckpoint = trim_after_checkpoint
    args.instantTriggerCompaction = True
    compactor_runner = CompactorRunner(args)
    compactor_runner.run()

def trigger_always_and_validate(args, port, config_file_path):
    set_logger(config_file_path)
    log_info("Trigger always and validate compactor with port " + str(port))
    status_before_cp, cycle_count_before_cp = get_compactor_status(port)
    if status_before_cp == "STARTED":
        log_info("Waiting for current compactor cycle to end")
        wait_for_compactor_end(port, cycle_count_before_cp)
    invoke_compactor_runner(args, port, config_file_path, False)
    target_cycle_count = cycle_count_before_cp + 1
    return wait_and_verify_compactor_success(port, target_cycle_count)

def trigger_if_required_and_validate(args):
    set_logger(args.compactorConfig)
    log_info("Trigger if required and validate compactor with port " + str(args.port))
    status_before_cp, cycle_count_before_cp = get_compactor_status(args.port)
    target_cycle_count = cycle_count_before_cp
    if status_before_cp != 'STARTED':
        log_info("Triggering compactor...")
        invoke_compactor_runner(args, args.port, args.compactorConfig, args.trimAfterCheckpoint)
        target_cycle_count = cycle_count_before_cp + 1
    # check if the cycle completed successfully
    if wait_and_verify_compactor_success(args.port, target_cycle_count) is False:
        log_info("ERROR: Compactor verification failed.")
        sys.exit(1)
    else:
        log_info("Compactor completed successfully")
