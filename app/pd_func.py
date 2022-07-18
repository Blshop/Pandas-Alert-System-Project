import pandas as pd
import datetime
from config import default_config

# column names for csv log files
df_columns = [
    "error_code",
    "error_message",
    "severity",
    "log_location",
    "mode",
    "model",
    "graphics",
    "session_id",
    "sdkv",
    "test_mode",
    "flow_id",
    "flow_type",
    "sdk_date",
    "publisher_id",
    "game_id",
    "bundle_id",
    "appv",
    "language",
    "os",
    "adv_id",
    "gdpr",
    "ccpa",
    "country_code",
    "date",
]


def read_df(path):
    """Return df sorted by date"""
    df = pd.read_csv(path, names=df_columns)
    df = df.sort_values(by="date")
    return df


def apply_options(df, options):
    """
    apply filter and groupby to DataFrame
    :param df: pandas DataFrame
    :param options: options for filtering and grouping data
    :return: pandas DataFrame
    """
    # select columns to filter, ad filter them one by one
    key_options = options.keys() - default_config
    for option in key_options:
        df = df[df[option].astype(str) == options[option]]

    # add dates for window function
    df.eval("start_date=date", inplace=True)
    df.eval("end_date=date", inplace=True)

    # cast interval to seconds
    interval = to_sec(options["interval"])

    # perform window function depending on groupby rule.
    if options["groupby"] == "":
        df = df.rolling(int(options["count"]) + 1).agg(
            {"start_date": "min", "end_date": "max"}
        )
    else:
        df = (
            df.groupby(options["groupby"])
            .rolling(int(options["count"]) + 1)
            .agg({"start_date": "min", "end_date": "max"})
        )

    df["diff"] = df["end_date"] - df["start_date"]
    df = df[df["diff"] < interval]
    return df


def log_results(df, options, section, path):
    """
    write result of the analysis to log file.
    :param df: pandas DataFrame to analyze
    :param options: list of analyze options
    :param section: string name of options group
    :path path: path where results are stored
    """
    with open(f"{path}{section}.txt", "a+") as result_log:
        end_date = 0
        if options["groupby"] != "":
            # choosing correct data for each value in groupby
            df = df.reset_index()
            groups = df[options["groupby"]].unique()
            for group in groups:
                # choosing only that data that wasn't used before
                for index, row in df[df[options["groupby"]] == group].iterrows():
                    if row["start_date"] > end_date:
                        end_date = row["end_date"]
                        result_log.write(
                            f'Event period from {to_datetime(row["start_date"])} to {to_datetime(row["end_date"])} for {group} \n'
                        )
        else:
            for index, row in df.iterrows():
                if row["start_date"] > end_date:
                    end_date = row["end_date"]
                    result_log.write(
                        f'Event period from {to_datetime(row["start_date"])} to {to_datetime(row["end_date"])}\n'
                    )


def to_datetime(sec):
    """
    Cast seconds to datetime round to 1s.
    """
    time = datetime.datetime.fromtimestamp(sec)
    time = pd.to_datetime(time).round("1s")
    return time


def to_sec(interval):
    """
    Cast time in format "%H:%M:%S" to seconds.
    """
    date_time = datetime.datetime.strptime(interval, "%H:%M:%S")
    a_timedelta = date_time - datetime.datetime(1900, 1, 1)
    interval = a_timedelta.total_seconds()
    return interval
