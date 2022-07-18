import time
import glob, os, sys
from watchdog.observers import Observer
from watchdog.events import PatternMatchingEventHandler
from config import read_config, check_config
from pd_func import read_df, apply_options, log_results


# const values fo paths.
CONFIG_PATH = "config.ini"
INPUT_LOGS_PATH = os.environ["INPUT_LOGS"]
OUTPUT_LOGS_PATH = os.environ["OUTPUT_LOGS"]


def scan_folder(path):
    """
    Search given folder for csv files
    :param path: path to folder to scan
    :return: return list of all csv files in folder sorted by date asc
    """
    print("Scanning folder for existing log files")
    list_of_log_files = filter(os.path.isfile, glob.glob(f"{path}*.csv"))
    list_of_log_files = sorted(list_of_log_files, key=os.path.getmtime)
    return list_of_log_files


def analyze_logs(log_df, config, file_path):
    """
    Analyze given csv file and write results in log file
    :param log_df: pandas dataframe
    :param config: analysis configuration
    :param file_path: log file path
    """
    print(f"File {file_path} analysis started")
    for section in config.sections():
        df = apply_options(log_df, config[section])
        log_results(df, config[section], section, OUTPUT_LOGS_PATH)
    print(f"File {file_path} analysis finished")


def on_created(event):
    """
    Catch event on csv file creation and analyze it.
    """
    # watchdog has no event on file copy finished,
    # so we chatch file creation and then wait until its modification time stop changing
    stamp = -1
    while stamp != os.stat(event.src_path).st_mtime:
        stamp = os.stat(event.src_path).st_mtime
        time.sleep(1)
    log_df = read_df(event.src_path)
    analyze_logs(log_df, config, event.src_path)


if __name__ == "__main__":

    # prepare config
    config = read_config(CONFIG_PATH)
    check_config(config)

    # read and analize already existing csv files
    list_of_log_files = scan_folder(INPUT_LOGS_PATH + "/")
    for file_path in list_of_log_files:
        log_df = read_df(file_path)
        analyze_logs(log_df, config, file_path)

    # monitoring folder for new csv files
    patterns = ["*.csv"]
    ignore_patterns = ["*~"]
    event_handler = PatternMatchingEventHandler(
        patterns=patterns, ignore_patterns=ignore_patterns, ignore_directories=True
    )
    event_handler.on_created = on_created
    observer = Observer()
    path = INPUT_LOGS_PATH
    observer.schedule(event_handler, path, recursive=True)
    observer.start()
    try:

        while True:
            time.sleep(1)
    finally:
        observer.stop()
        observer.join()
