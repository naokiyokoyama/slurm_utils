import subprocess
import time

import numpy as np
import pandas as pd

SLURM_CMD = 'squeue -u $USER --format "%.100i %.100P %.100j %.100t %.100R"'
COOLDOWN_SECONDS = 120
# Header looks like this:
#    JOBID  PARTITION                     NAME    ST NODELIST(REASON)


def line_to_list(line):
    # Replace any single spaces with a dummy value
    final_line = ""
    for idx, letter in enumerate(line):
        if letter != " " or idx > 0 and line[idx - 1] != " ":
            final_line += letter
    return final_line.split(" ")


class SlurmMonitor:
    def __init__(self, cooldown_seconds=COOLDOWN_SECONDS):
        self.last_peek = 0
        self.latest_info = None
        self.cooldown_seconds = cooldown_seconds

    def refresh(self, silent=False):
        if time.time() - self.last_peek > self.cooldown_seconds:
            data = subprocess.check_output(SLURM_CMD, shell=True).decode("utf-8")
            self.last_peek = time.time()
            self.latest_info = pd.DataFrame(
                [line_to_list(x) for x in data.split("\n")[1:-1]],
                columns=[x for x in data.split("\n")[0].split()],
            )
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

    @staticmethod
    def print(*args):
        print("[slurm_utils.monitor]:", *args)
