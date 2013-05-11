''' misc utilities
'''
import subprocess
import os
import time
from org.apache.helix.util.UserExceptions import TimeoutException
from org.apache.helix.util.logger import get_logger


def enum(*sequential, **named):
    enums = dict(zip(sequential, range(len(sequential))), **named)
    reverse = dict((value, key) for key, value in enums.iteritems())
    enums['reverse_mapping'] = reverse
    def reverse_m(value):
        return reverse[value]
    enums['toString'] = staticmethod(reverse_m)
    return type('Enum', (), enums)


def explicit_enum(**enums):
    reverse = dict((value, key) for key, value in enums.iteritems())
    enums['reverse_mapping'] = reverse
    def reverse_m(value):
        return reverse[value]
    enums['toString'] = staticmethod(reverse_m)
    return type('Enum', (), enums)

def ternary(cond1, result1, result2):
    if cond1:
        return result1
    else:
        return result2


'''
  Run an external command
'''
class ExternalCommand(object):
    logger = get_logger(__name__)

    @staticmethod
    def executeWithTimeout(workingDir, cmdValue, args):
        return ExternalCommand.executeWithTimeout(workingDir, cmdValue, 0, args)

    @staticmethod
    def executeWithTimeout(workingDir, cmdValue, timeoutValue, args):
        cmd = "%s %s" % (cmdValue, " ".join(args))
        cmd_list = [cmdValue]
        if args:
            cmd_list.extend(cmd_list)
        cwd = os.getcwd()
        os.chdir(workingDir)
        # return os.system(cmds)
        #print "cmds = %s" % cmds
        subProc = subprocess.Popen(cmd_list, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, close_fds=True, shell=True)
        slept = 0
        sleepInterval = 1
        while (timeoutValue == 0 or slept < timeoutValue):
            if subProc.poll() is not None:
                break
            time.sleep(sleepInterval)
            slept += sleepInterval
        if slept >= timeoutValue and timeoutValue >0:
            subProc.kill()
            time.sleep(1)
            raise TimeoutException("cmd = %s" % cmd)
        elif subProc.returncode != 0:
            print "OUTPUT=\n" + "".join(subProc.stdout.readlines())
            return subProc.returncode
