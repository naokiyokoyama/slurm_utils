import os
import subprocess
import time

import numpy as np
import pandas as pd

SLURM_CMD = 'squeue -u $USER --format "%.100i %.100P %.100j %.100t %.100R"'
COOLDOWN_SECONDS = 120
# Header looks like this:
#    JOBID  PARTITION                     NAME    ST NODELIST(REASON)

DUMMY_VALUE = "4123kjhgkjhg156298"


def line_to_list(line):
    final_line = ""
    for idx, letter in enumerate(line):
        if letter != " ":
            final_line += letter
        elif letter == " " and not (
            idx > 0 and line[idx - 1] == " " or idx < len(line) and line[idx + 1] == " "
        ):
            final_line += DUMMY_VALUE
        else:
            final_line += " "
    return [i.replace(DUMMY_VALUE, " ") for i in final_line.split()]


class SlurmMonitor:
    def __init__(self, cooldown_seconds=COOLDOWN_SECONDS):
        self.last_peek = 0
        self.latest_info = None
        self.silence_prints = False
        self.cooldown_seconds = cooldown_seconds

    def refresh(self, silent=False, force=False):
        if force or time.time() - self.last_peek > self.cooldown_seconds:
            self.last_peek = time.time()
            self.latest_info = get_squeue_data()
            return True
        else:
            if not silent:
                self.print(
                    f"{self.cooldown_seconds} "
                    "has not passed since last refresh; can't refresh yet"
                )
            return False

    def get_all_job_names(self):
        return self.latest_info["NAME"].tolist()

    def get_all_job_ids(self):
        return self.latest_info["JOBID"].tolist()

    def job_name_to_id(self, job_name):
        job_names = self.get_all_job_names()
        num_matches = job_names.count(job_name)
        if num_matches == 1:
            row_index = np.where(self.latest_info["NAME"] == job_name)[0][0]
            return self.latest_info["JOBID"][row_index]
        elif job_name not in job_names:
            self.print(f"Job name '{job_name}' not found!")
        else:
            self.print(f"More than one match ({num_matches}) were found for {job_name}")
        return None

    def get_job_info(self, job_id, by_job_name=False):
        if by_job_name:
            job_id = self.job_name_to_id(job_id)
            if job_id is None:
                return None
        row = self.latest_info[self.latest_info["JOBID"] == job_id]
        return row.to_dict("records")[0]

    def print(self, *args):
        if not self.silence_prints:
            print("[slurm_utils.monitor]:", *args)


def get_squeue_data():
    """
    Executes squeue command for current user and returns results as a pandas DataFrame.

    Returns:
        pandas.DataFrame: Contains job queue information with columns:
        JOBID, PARTITION, NAME, ST (status), and NODELIST(REASON)
    """
    # Run the squeue command and capture output
    cmd = [
        "squeue",
        "-u",
        os.environ["USER"],
        "--format",
        "%.100i %.100P %.100j %.100t %.100R",
    ]
    result = subprocess.run(cmd, capture_output=True, text=True)

    # Split output into lines
    lines = result.stdout.strip().split("\n")

    # Find the column positions by looking at the header
    header = lines[0]
    col_positions = []
    start = 0

    # Get the starting position of each column
    for col in ["JOBID", "PARTITION", "NAME", "ST", "NODELIST(REASON)"]:
        pos = header.find(col, start)
        col_positions.append(pos)
        start = pos + 1

    # Add the end position
    col_positions.append(len(header))

    # Parse each line using the column positions
    data = []
    for line in lines[1:]:  # Skip header
        row = []
        for i in range(len(col_positions) - 1):
            start = col_positions[i]
            end = col_positions[i + 1]
            value = line[start:end].strip()
            row.append(value)
        data.append(row)

    # Create DataFrame with the correct column names
    df = pd.DataFrame(
        data, columns=["JOBID", "PARTITION", "NAME", "ST", "NODELIST(REASON)"]
    )

    return df
