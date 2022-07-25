import pandas as pd
import datetime as dt
import os

# Task: 
# -- Calculate the time a client's policy is valid for (Days from Suspension)
# -- First collection per agent, per day, and per payment type
# -- Total collection per payment type

# IO:
sample_input = "2019_02_02_payments.csv"
sample_output = "output"

agent_collection_report_name = "agent_collection_report.csv"
payment_types_report_name = "payment_types.csv"
days_from_susp_report_name = "days_from_suspension_report.csv"

# TODO
# -- Add archiving of input files
# -- Input via command line
# -- Folder structure for neatness and scalability
# -- Add tests

script_dir = os.path.dirname(os.path.realpath(__file__))

def valid_input_file(file_path):
    """
    Checks the input file exists.

    :param file_path: str
    :return: bool
    """
    return os.path.isfile(file_path)

def create_output_path(output_folder, file_name):
    """
    Creates the output folder if it does not yet exist.
    Returns the full file path to the output file.

    :param output_folder: str
    :param file_name: str
    :return: str
    """
    output_path = os.path.join(script_dir, output_folder)
    if not os.path.exists(output_path):
        os.makedirs(output_path)
    return os.path.join(output_path, file_name)

def clean_df(file_name):
    """
    Creates a DataFrame from the input csv after checking the file exists.
    Raises ValueError if the file does not exist.
    Filters out all rows that do not have status == SUCCESSFUL

    :param file_name: str
    :return: pandas.DataFrame
    """
    file_path = os.path.join(script_dir, "input-files", file_name)
    if not valid_input_file(file_path):
        raise ValueError("Input file not valid.")
    
    df = pd.read_csv(file_path)
    # Filter out payments that aren't 'SUCCESSFUL'
    df = df[df["status"] == "SUCCESSFUL"]
    # 'CLIENT_REFERRAL' count as payment?
    return df

def get_ref_date(file_name):
    """
    Gets the date from an input file name.

    :param file_name: str
    :return: datetime.datetime

    >>> get_ref_date('2022_06_12_payments.csv')
    datetime.datetime(2022, 6, 12, 0, 0)
    """
    try:
        ref_date = dt.datetime.strptime(file_name, '%Y_%m_%d_payments.csv')
        return ref_date
    except:
        raise ValueError("Invalid file name.")

def generate_agent_collection_report(df, output_folder):
    """
    Generates the agent_collection_report.

    :param df: pandas.DataFrame
    :param output_folder: str
    :return: null
    """
    df["date"] = pd.to_datetime(df["created"]).dt.date
    sum_df = df.groupby(by=["agent_user_id", "date", "payment_type"], as_index=False, sort=True)["payment_amount"].sum()
    output_path = create_output_path(output_folder, agent_collection_report_name)
    sum_df.to_csv(output_path, index=False, header=["agent_user_id", "date", "payment_type", "total_amount"])

def generate_payment_types_report(df, output_folder):
    """
    Generates the payment_types report.

    :param df: pandas.DataFrame
    :param output_folder: str
    :return: null
    """
    payment_types = df.groupby("payment_type")["payment_amount"].sum().sort_index()
    output_path = create_output_path(output_folder, payment_types_report_name)
    payment_types.to_csv(output_path)

def generate_days_from_suspension_report(df, output_folder, ref_date):
    """
    Generates the 'days_from_suspension' report, relative to the date of the input file.

    :param df: pd.DataFrame
    :param output_folder: str
    :param ref_date: dt.datetime.date
    :return: null
    """
    def return_policy_status(last_payment_date, current_date = dt.datetime.now().date()):
        last_payment = pd.to_datetime(last_payment_date).date()
        days_ago = (current_date - last_payment).days
        days_from_susp = 91 - days_ago
        return days_from_susp

    def days_from_suspension(input_group):
        test_input_date = input_group["created"].values[0]
        days_from_susp = return_policy_status(test_input_date, ref_date.date())
        return days_from_susp
    
    new_df = df.groupby("device_id").apply(func=days_from_suspension).to_frame()
    new_df.set_axis(["days_from_suspension"], inplace=True, axis='columns')
    new_df.sort_values(by="days_from_suspension", inplace=True)
    output_path = create_output_path(output_folder, days_from_susp_report_name)
    new_df.to_csv(output_path)

def generate_all_reports(input_file, output_folder):
    """
    Generates all three reports currently available.

    :param input_file: str
    :param output_folder: str
    :return: null
    """
    df = clean_df(input_file)
    ref_date = get_ref_date(input_file)
    generate_agent_collection_report(df, output_folder)
    generate_payment_types_report(df, output_folder)
    generate_days_from_suspension_report(df, output_folder, ref_date)


if __name__ == "__main__":
    import doctest
    doctest.testmod()
    generate_all_reports(sample_input, sample_output)