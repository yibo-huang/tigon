import subprocess as sp
import os
import sys
from typing import List, Optional

Fail_MSG = 'Failed to run command: '


def run_local_command_parallel(cmds: List[List[str]], env: Optional[dict] = None):
    if env:
        new_env = dict(os.environ, **env)
    else:
        new_env = os.environ
    running_procs = []
    for cmd in cmds:
        print(f"{' '.join(cmd)}")
        p = sp.Popen(cmd, stdout=sp.PIPE, stderr=sp.PIPE, encoding='utf8', env=new_env)
        running_procs.append((p, cmd))
    for (p, cmd) in running_procs:
        returncode = p.wait()
        if returncode != 0:
            outs, errs = p.communicate()
            raise RuntimeError(f'Fail to run cmd: {" ".join(cmd)}\n{errs}')


def copy_remote_to_local(ssh_str, remote_path, local_path, port=22):
    complete_cmd = ['scp', '-P', f"{port}", '-q', '-oStrictHostKeyChecking=no', '-r', f"{ssh_str}:{remote_path}", local_path]
    print(f"{' '.join(complete_cmd)}")
    ret = sp.run(complete_cmd, encoding='utf8')
    if ret.returncode != 0:
        raise Exception(f'Failed to copy from {ssh_str}:{remote_path} to {local_path}')


def copy_local_to_remote(ssh_str, local_path, remote_path, port=22):
    complete_cmd = ['scp', '-P', f"{port}", '-q', '-oStrictHostKeyChecking=no', '-r', local_path, f"{ssh_str}:{remote_path}"]
    print(f"{' '.join(complete_cmd)}")
    ret = sp.run(complete_cmd, encoding='utf8')
    if ret.returncode != 0:
        raise Exception(f'Failed to copy from {local_path} to {ssh_str}:{remote_path}')


def run_remote_command(ssh_str: str, cmd: List[str], port=22):
    complete_cmd = ['ssh', '-q', '-oStrictHostKeyChecking=no', "-p", f"{port}", ssh_str, '--'] + cmd
    print(f"{' '.join(complete_cmd)}")
    ret = sp.run(complete_cmd,
                 stdout=sp.PIPE, stderr=sp.PIPE, encoding='utf8')
    if ret.returncode != 0:
        raise RuntimeError(Fail_MSG + ' '.join(cmd) + '\n' + ret.stderr)
    return ret


def run_remote_shell_cmd(ssh_str: str, cmd: str, port=22):
    complete_cmd = f'ssh -q -oStrictHostKeyChecking=no -p {port} {ssh_str} -- ' + cmd
    print(complete_cmd)
    ret = sp.run(complete_cmd,
                 stdout=sp.PIPE, stderr=sp.PIPE, encoding='utf8', shell=True)
    if ret.returncode != 0:
        raise RuntimeError(Fail_MSG + ''.join(cmd) + '\n' + ret.stderr)
    return ret


def run_local_shell_command(cmd: str, allow_fail=False):
    print(cmd)
    ret = sp.run(cmd,
                 stdout=sp.PIPE, stderr=sp.PIPE, encoding='utf8', shell=True)
    if not allow_fail and ret.returncode != 0:
        raise RuntimeError(Fail_MSG + cmd + '\n' + ret.stderr)
    return ret


def run_local_command(cmd: List[str], env: Optional[dict] = None, capture = True, allow_fail=False):
    if env:
        new_env = dict(os.environ, **env)
    else:
        new_env = os.environ
    print(f"{' '.join(cmd)}")
    if capture:
        ret = sp.run(cmd, stdout=sp.PIPE, stderr=sp.PIPE,
                     encoding='utf8', env=new_env)
    else:
        ret = sp.run(cmd, encoding='utf8', stderr=sys.stderr, stdout=sys.stdout,
                     env=new_env)
    if not allow_fail and ret.returncode != 0:
        if capture:
            raise RuntimeError(Fail_MSG + ' '.join(cmd) + '\n' + ret.stderr)
        else:
            raise RuntimeError(Fail_MSG + ' '.join(cmd) + '\n')
    return ret


def run_local_command_allow_fail(cmd: List[str], capture = True):
    print(f"{' '.join(cmd)}")
    if capture:
        ret = sp.run(cmd, stdout=sp.PIPE, stderr=sp.PIPE, encoding='utf8')
    else:
        ret = sp.run(cmd, encoding='utf8')
    return ret.stdout, ret.stderr
