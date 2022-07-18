import configparser
from pd_func import df_columns
import sys


# default columns in config
default_config = ["groupby", "count", "interval"]


def read_config(path):
    """Return sets of rules from given path on success, otherwise terminate script"""
    try:
        config = configparser.ConfigParser()
        print("Reading config file")
        config.read(path)
        return config
    except Exception as e:
        print(e)
        sys.exit()


def check_config(config):
    """
    Check config to contain all valid parameters.
    """
    all_columns = df_columns + default_config
    for rule in config.sections():

        # check that all parameters to sort has existiong values
        if not all(option in all_columns for option in config.options(rule)):
            print(f"There is an error in {rule}")
            sys.exit()

        # check that groupby parameter has correct values
        if (
            config[rule]["groupby"] not in df_columns
            and config[rule]["groupby"] != config["DEFAULT"]["groupby"]
        ):
            print(f"{rule} groupby contains inappropriate value")
            sys.exit()
