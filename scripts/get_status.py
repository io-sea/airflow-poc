import subprocess
import sys
import time

try:
    session_name = sys.argv[1]
    status_to_control = sys.argv[2]
    time_out = int(sys.argv[3])
except:
    print("Error - arguments")
    exit (-1)

time_counter = 0
end_loop = False

while time_counter <= time_out:
    p = subprocess.Popen('iosea-wf status', shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    
    for line in p.stdout.readlines():
        line = line.decode("utf-8")
        if(session_name in line):
            print(line)
            if(status_to_control in line):
                print("yes")
                end_loop = True
                
    time.sleep(5)
    time_counter = time_counter + 5;    
    if(end_loop == True):
        break

if(end_loop == False):
    print("Error - time_out")
    exit(-2)

