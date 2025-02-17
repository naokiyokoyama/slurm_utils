import os
import subprocess
import time
import pandas as pd
import numpy as np

COOLDOWN_SECONDS = 120


def get_squeue_data():
    """
    Executes squeue command for current user and returns results as a pandas DataFrame.
    Uses a tab separator for reliable parsing without JSON.
    """
    cmd = f'squeue -u {os.environ["USER"]} -o "%i\\t%P\\t%j\\t%t\\t%R"'
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)

    try:
        # Split output into lines and parse using tabs as delimiter
        lines = result.stdout.strip().split("\n")

        # Create DataFrame using tab separator
        df = pd.DataFrame(
            [line.split("\\t") for line in lines[1:]],  # Skip header
            columns=["JOBID", "PARTITION", "NAME", "ST", "NODELIST(REASON)"],
        )

        return df
    except Exception as e:
        print(f"Error parsing squeue output: {e}")
        return pd.DataFrame(
            columns=["JOBID", "PARTITION", "NAME", "ST", "NODELIST(REASON)"]
        )


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
