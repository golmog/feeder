"""Linux에서 부모 워커 종료 시 Rclone도 종료하도록 설정한 뒤 exec한다."""

import ctypes
import os
import signal
import sys


def main():
    parent = int(sys.argv[1])
    command = sys.argv[2:]
    if not command or not os.path.isabs(command[0]):
        raise SystemExit(125)
    libc = ctypes.CDLL(None, use_errno=True)
    if libc.prctl(1, signal.SIGKILL, 0, 0, 0) != 0:
        raise SystemExit(125)
    if os.getppid() != parent:
        raise SystemExit(125)
    os.execv(command[0], command)


if __name__ == '__main__':
    main()
