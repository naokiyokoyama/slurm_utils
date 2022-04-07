import subprocess


def add_underscore_to_keys(in_dict):
    return {"--" + k if len(k) > 2 else "-" + k: v for k, v in in_dict.items()}


def submit_job(job_path, env_vars=None, other_args=None, **kwargs):
    if other_args is None:
        other_args_copy = {}
    else:
        other_args_copy = add_underscore_to_keys(other_args)
    args = ""
    other_args_copy.update(add_underscore_to_keys(kwargs))
    if env_vars is not None and len(env_vars) > 0:
        env_vars_list = [f"{k}={v}" for k, v in env_vars.items()]
        args += "--export ALL," + ",".join(env_vars_list) + " "
    if len(other_args_copy) > 0:
        other_args_list = [f"{k}={v}" for k, v in other_args_copy.items()]
        args += " ".join(other_args_list) + " "
    cmd = " ".join(f"sbatch {args} {job_path}".split())
    print("[slurm_utils.submit]: Executing:", cmd)
    subprocess.check_call(cmd, shell=True)
