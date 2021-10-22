import subprocess, sys, threading

if __name__ == '__main__':
        ## command to run - tcp only ##
    cmd = "test.bat"
     
    def run_bat():
        p = subprocess.Popen(cmd, shell = True, stdout = subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
        # while True:
            # out = p.stdout.read(1)
            # if out == '' and p.poll() != None:
                # print('break')
                # break
            # if out != '':
                # sys.stdout.write(out)
                # sys.stdout.flush()
        for stdout_line in iter(p.stdout.readline, ""):
            yield stdout_line 
        p.stdout.close()
        return_code = p.wait()
        if return_code:
            raise subprocess.CalledProcessError(return_code, cmd)
     
    i = 0
    ## run it ##
    for line in run_bat():
        print(i, line)
        i += 1
     
    ## But do not wait till netstat finish, start displaying output immediately ##
