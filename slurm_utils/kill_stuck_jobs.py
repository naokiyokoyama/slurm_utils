"""
This script will periodically poll a folder. New files will appear in this folder, where
the ID of the slurm job is the name of the file. This script will kill a job if neither
its output nor error file has been updated in a while.

Critically, we assume that sbatch was run in the same directory as the output and error
files, and that they have the extensions .out and .err, respectively. Having just one
of either is allowed.

This script is expected to run in the background indefinitely.
"""

import glob
import json
import os
import os.path as osp
import subprocess
import time

POLL_FOLDER = osp.join(os.environ["HOME"], "slurm_poll_stuck_jobs")
if not osp.exists(POLL_FOLDER):
    os.mkdir(POLL_FOLDER)


def poll(output_file: str = ""):
    from slurm_utils.monitor import SlurmMonitor

    # cooldown = 60 * 5  # 5 minutes
    cooldown = 60 * 1  # 1 minute
    monitor = SlurmMonitor(cooldown)
    first_time = True
    while True:
        monitor.refresh()
        slurm_job_files = glob.glob(osp.join(POLL_FOLDER, "*.slurm_job"))

        def get_job_id(file):
            with open(file, "r") as f:
                slurm_json = json.load(f)
            return int(slurm_json["SLURM_JOB_ID"])

        slurm_job_files = sorted(slurm_job_files, key=lambda x: -get_job_id(x))
        for file in slurm_job_files:
            with open(file, "r") as f:
                slurm_json = json.load(f)

            job_id = slurm_json["SLURM_JOB_ID"]
            error_file = get_error_file(slurm_json)

            if error_file is None:
                error_file_contents = None
            else:
                with open(error_file, "r") as f:
                    error_file_contents = f.read()

            if error_file_contents is not None and "Error" in error_file_contents:
                print("Replacing 'Error' w/ 'ERROR' for job", job_id)

                error_line = None
                for line in error_file_contents.splitlines():
                    if "Error" in line:
                        error_line = line
                        break
                assert error_line is not None

                # Notify user about the error
                error_msg = (
                    f" '{slurm_json['SLURM_JOB_NAME']}' ({job_id}): " + error_line
                )

                # Replace 'Error' with 'ERROR'
                error_file_contents = error_file_contents.replace("Error", "ERROR")
                with open(error_file, "w") as f:
                    f.write(error_file_contents)

                if job_id in monitor.get_all_job_ids():
                    # Kill the job and resubmit it
                    # TODO: don't assume train.sh
                    sbatch_script = "train.sh"
                    sbatch_script_full = osp.join(
                        slurm_json["SLURM_SUBMIT_DIR"], sbatch_script
                    )
                    subprocess.check_call(f"scancel {job_id}", shell=True)

                    subprocess.check_call(
                        f"sbatch {sbatch_script}",
                        shell=True,
                        cwd=slurm_json["SLURM_SUBMIT_DIR"],
                    )
                    os.remove(file)
                else:
                    sbatch_script_full = None
                    print(
                        f"Job {job_id} not in {monitor.get_all_job_ids()}, "
                        "so not resubmitting."
                    )

                if job_id not in monitor.get_all_job_ids():
                    os.remove(file)  # remove stale files for past jobs

                # Notify user about the error
                full_error_msg = "Error found:" if first_time else "ERROR FOUND:"
                full_error_msg += (
                    " (NOT resubmitted):"
                    if sbatch_script_full is None
                    else " (resubmitted):"
                )
                full_error_msg += error_msg
                print(full_error_msg)
                if output_file != "":
                    with open(output_file, "a") as f:
                        f.write(full_error_msg + "\n")

            if job_id not in monitor.get_all_job_ids() and osp.isfile(file):
                os.remove(file)  # remove stale files for past jobs

        time.sleep(cooldown)
        first_time = False


def get_error_file(slurm_json):
    """If there is a .err file, return that if it's the only one. Otherwise, return
    the .out file, if it's the only one. Otherwise, return None."""
    submit_dir = slurm_json["SLURM_SUBMIT_DIR"]
    possible_files = glob.glob(osp.join(submit_dir, "*.err"))
    if len(possible_files) == 1:
        return possible_files[0]
    possible_files = glob.glob(osp.join(submit_dir, "*.out"))
    if len(possible_files) == 1:
        return possible_files[0]
    return None


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
        poll(args.output_file)
