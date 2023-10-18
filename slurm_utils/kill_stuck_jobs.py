"""
This script will periodically poll a folder. New files will appear in this folder, where
the ID of the slurm job is the name of the file.

This script will detect if a job's output file has not been updated in a while, or if an
error has occurred in it. If the job is still running (i.e., it is somehow stuck), then
this script will kill it. It will then resubmit the job (whether it was stuck or not).

Critically, we assume the following:
1. The slurm job is running a script called 'train.sh'.
2. The job was submitted ('sbatch') from the same directory as the script 'train.sh'.
3. Errors will be written to a file ending in '.err' or '.out', and that file will be
    located in the same directory as the script 'train.sh'.

This script is expected to run in the background indefinitely.
"""

import glob
import json
import os
import os.path as osp
import subprocess
import time
from typing import List, Tuple, Dict

POLL_FOLDER = osp.join(os.environ["HOME"], "slurm_poll_stuck_jobs")
if not osp.exists(POLL_FOLDER):
    os.mkdir(POLL_FOLDER)


class ErrorMonitor:
    """
    Keeps track of the contents of error files. If the contents of an error file
    changes, then we know that the job is still running. If the contents of an error
    file does not change for a long time, then we know that the job is stuck.
    """

    def __init__(self, stale_time: int = 60 * 30):
        self._stale_time = stale_time
        self._error_contents: Dict[int, Tuple[float, str]] = {}

    def is_stale(self, job_id: int, error_file_contents: str) -> bool:
        """
        Determines whether the error file contents are stale. If the error file
        contents are stale, then the job is stuck. It will also update the error file
        contents in the dictionary.

        Args:
            job_id: The job id.
            error_file_contents: The contents of the error file.

        Returns:
            True if the error file contents are stale, False otherwise.
        """
        if job_id not in self._error_contents:
            self._error_contents[job_id] = (time.time(), error_file_contents)
            return False
        last_time, last_contents = self._error_contents[job_id]
        if last_contents != error_file_contents:
            self._error_contents[job_id] = (time.time(), error_file_contents)
            return False
        if time.time() - last_time > self._stale_time:
            return True
        return False

    def purge_inactive(self, job_ids: List[int]) -> None:
        """
        Purges the job ids that are not in the list of job ids.

        Args:
            job_ids: The list of job ids.
        """
        self._error_contents = {
            job_id: (last_time, last_contents)
            for job_id, (last_time, last_contents) in self._error_contents.items()
            if job_id in job_ids
        }


class StuckJobKiller:
    def __init__(self, output_file: str = ""):
        from slurm_utils.monitor import SlurmMonitor

        self._cooldown = 60 * 5  # 1 minute
        self._monitor = SlurmMonitor(self._cooldown)
        self._first_time = True
        self._error_monitor = ErrorMonitor()
        self._output_file = output_file

    @staticmethod
    def _get_job_id(file: str) -> int:
        with open(file, "r") as f:
            slurm_json = json.load(f)
        return int(slurm_json["SLURM_JOB_ID"])

    @staticmethod
    def _get_error_file_contents(slurm_json: dict) -> Tuple[str, str]:
        """If there is a .err file, return that if it's the only one. Otherwise, return
        the .out file, if it's the only one. Otherwise, return ''."""
        submit_dir = slurm_json["SLURM_SUBMIT_DIR"]
        for ext in ["*.err", "*.out"]:
            possible_files = glob.glob(osp.join(submit_dir, ext))
            if len(possible_files) == 1:
                file_path = possible_files[0]
                with open(file_path, "r") as f:
                    return file_path, f.read()
        return "", ""

    @staticmethod
    def _get_error_line(error_file_contents: str) -> str:
        for line in error_file_contents.splitlines():
            if "Error" in line:
                return line
        raise ValueError("No error line found in error file contents.")

    def _get_slurm_job_ids_and_files(self) -> List[Tuple[int, str]]:
        slurm_files = glob.glob(osp.join(POLL_FOLDER, "*.slurm_job"))
        slurm_job_ids_and_files = [(self._get_job_id(sf), sf) for sf in slurm_files]

        # Sort the list of tuples by job id
        slurm_job_ids_and_files = sorted(slurm_job_ids_and_files, key=lambda x: -x[0])
        return slurm_job_ids_and_files

    def poll(self):
        self._monitor.refresh()
        slurm_job_ids_and_files = self._get_slurm_job_ids_and_files()
        active_job_ids = [int(i) for i in self._monitor.get_all_job_ids()]

        # Purge the error monitor of inactive jobs
        self._error_monitor.purge_inactive(active_job_ids)

        for job_id, file in slurm_job_ids_and_files:
            with open(file, "r") as f:
                slurm_json = json.load(f)
            error_file, error_file_contents = self._get_error_file_contents(slurm_json)

            error_occurred = True
            error_msg = f" '{slurm_json['SLURM_JOB_NAME']}' ({job_id}): "
            if "Error" in error_file_contents:
                print("Replacing 'Error' w/ 'ERROR' for job", job_id)
                error_msg += self._get_error_line(error_file_contents)

                # Replace 'Error' with 'ERROR' within the error file, so it doesn't
                # trigger this script again
                error_file_contents = error_file_contents.replace("Error", "ERROR")
                with open(error_file, "w") as f:
                    f.write(error_file_contents)
            elif self._error_monitor.is_stale(job_id, error_file_contents):
                # Check if the file has been updated recently. We cannot count on using
                # the modification time of the file, since it may be updated by the
                # different nodes with different times. Instead, we check if the file
                # contents have changed since the last time we checked.
                error_msg += "Job is stuck."

                # If the job is stuck, we need to delete its .slurm_job file, so that
                # this script doesn't keep checking it.
                try:
                    os.remove(file)
                except FileNotFoundError:
                    pass
            else:
                error_occurred = False

            if error_occurred:
                # TODO: don't assume sbatch script is always 'submit_dir/train.sh'
                sbatch_script = "train.sh"

                if job_id in active_job_ids:  # kill the job
                    subprocess.check_call(f"scancel {job_id}", shell=True)
                subprocess.check_call(
                    f"sbatch {sbatch_script}",
                    shell=True,
                    cwd=slurm_json["SLURM_SUBMIT_DIR"],
                )

                # Notify user about the error
                full_error_msg = (
                    f"{'Error found ' if self._first_time else 'ERROR FOUND '}"
                    f"{'(NOT resubmitted): ' if self._first_time else '(resubmitted):'}"
                    f"{error_msg}\n"
                )
                if not self._first_time:
                    sbatch_script_full = osp.join(
                        slurm_json["SLURM_SUBMIT_DIR"], sbatch_script
                    )
                    full_error_msg += f"Resubmitted script: {sbatch_script_full}."
                print(full_error_msg)

                if self._output_file != "":
                    with open(self._output_file, "a") as f:
                        f.write(full_error_msg + "\n")

            if job_id not in active_job_ids and osp.isfile(file):
                # Check if the file has not been modified for over 150 hours. If so,
                # delete it.
                if time.time() - osp.getmtime(file) > 60 * 60 * 150:
                    print("Deleting", file)
                    os.remove(file)  # remove stale files for past jobs

        self._first_time = False
        time.sleep(self._cooldown)


def register(name):
    # Just dump all the slurm environment variables to a file
    slurm_vars = {k: v for k, v in os.environ.items() if k.startswith("SLURM")}
    if not slurm_vars:
        print(
            "No slurm environment variables found. Are you running this script"
            " from a slurm job?"
        )
        return
    job_id = slurm_vars["SLURM_JOB_ID"]
    filename = osp.join(POLL_FOLDER, f"{job_id}.slurm_job")
    with open(filename, "w") as f:
        json.dump(slurm_vars, f)
    print("Saved slurm environment variables to", filename)
    if name != "":
        print("Name I received:", name)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("-r", "--register", action="store_true")
    parser.add_argument("-n", "--name", type=str, default="")
    parser.add_argument(
        "-o",
        "--output-file",
        type=str,
        default="",
        help="Where error messages will be written to.",
    )
    args = parser.parse_args()
    if args.register:
        register(args.name)
    else:
        skj = StuckJobKiller(args.output_file)
        while True:
            skj.poll()
