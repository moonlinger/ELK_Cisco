#!/usr/bin/python
#title           :elk_collect_cmc.py
#description     :This script collects some CMC stats. Currently it collects the
#                 stats for
#                 - CPU Utilization
#                 - Memory total/free/used/buffercached
#                 - Data Port Rx/Tx
#author          :Ziva Zhen Zhang
#date            :20161118
#version         :1
#usage           :nohup /opt/cisco/elk_collect/elk_collect_cmc.py &
#notes           :
#python_version  :2.6.6
#history
#7. 20170428 V7
# - Change from using 'df -Th /var/lib/cassandra' to 'du -d 0 /var/lib/cassandra/'.
#6. 20170427 V6
# - Change from using 'du -h /var/lib/cassandra |tail -1' to 'df -Th /var/lib/cassandra'.
# - Add DB size percentage
# - Add stats for table metadata key num
# 5. 20170302 V5
# - After changing to CMC, the db partition is change from /arroyo/db/cassandra to /var/lib/cassandra
# - Change stats log from /arroyo/log to /var/log/elk_collect/
# 4. 20161130 V4 Add exception catch for unknown error
# 3. 20161125 V3 Add support for /arroyo/db size
# 2. 20161122 V2 Add support for NIC Rx/Tx
# 1. 20161118 V1 First version. Iherit from elk_collect_cmc.py
#============================
import commands
import time
import re
import json
import os

log_cmc_stats="/var/log/elk_collect/elk_collect_cmc_stats.log"

# 2 top operations take 2 sec
interval=8
time_pre_db=0
time_cur_db=0
write_count_pre=0
read_count_pre=0
read_count_cur=0
write_count_cur=0

nic1_tx_byte_pre=0
nic1_rx_byte_pre=0 


#Main
i=1

dir = os.path.dirname(log_cmc_stats)
if not os.path.exists(dir):
        os.makedirs(dir)
        
while 1 :
    try:    
        f_stats = open(log_cmc_stats, "a")
        # 1. Print No and UTC time 
        f_stats.write("ELKCassandraStatsNo " + str(i))
        f_stats.write(": "+ commands.getoutput('date -u "+%Y-%m-%dT%H:%M:%S UTC"'))
    
        # 2. Retrieve CPU Util Summary
        f_stats.write(", CPU ")
     
        cpu_output=commands.getoutput('top -b -n 2 -d 1|grep Cpu|tail -1').split( )
    except:
        print "CPU Output - Unexpected error:"
        pass

    # Get the 2nd top output
    try:
        index=cpu_output.index('%Cpu(s):')
        
        # Caluate CPU total utilization uses (100- CPU Idle)
        cpu_util=100 - float((cpu_output[7]))
        f_stats.write(str(cpu_util))
        f_stats.write(" ut,") 
    except ValueError:
        print(cpu_output)
        pass
        continue
    except:
        print "CPU Idle Retrieve - Unexpected error:"
        pass

    for j in range((1), (17)):
        try:
            f_stats.write(" "+ str(cpu_output[j]))
        except IndexError:
            print (cpu_output)
            pass
            break
        except:
          print "All CPU - Unexpected error:"
          pass

    # 3. Retrieve Memory Usage
    try:
        mem_output=commands.getoutput('top -b -n 2 -d 1|grep "KiB Mem"|tail -1').replace("+","0 ").replace("KiB Mem","KiBMem").split( )
        f_stats.write(", KiBMem") 
        index=mem_output.index('KiBMem')
    except ValueError:
        print(mem_output)
        pass
        continue
    except:
        print "Retrieve Memory Usage - Unexpected error:"
        pass

    for j in range((index+2), (index+10)):
        try:
            f_stats.write(" "+ str(mem_output[j]))
        except IndexError:
            print (mem_output)
            pass
            break
        except:
          print "Retrieve Memory Usage - Unexpected error:"
          pass

    # 4. Retrieve process Cassandra: CPU Util, Virtue & Physical Memory
    try:
        cass_output=commands.getoutput('top -b -n 2 -d 1|egrep "cassand0|cassand+"|tail -2').split( )
        f_stats.write(", Process cassandra CPU " + str(cass_output[8]))
        f_stats.write("%")
        f_stats.write(" VIRT " + str(cass_output[4]))
        f_stats.write(" RES " + str(cass_output[5]))
    except ValueError:
        f_stats.write(", Process cassandr CPU 0% VIRT 0 RES 0")
        pass
    except:
        print "Cassandra CPU Util Virtue & Physical Memory - Unexpected error:"
        pass

    # 5. Retrieve cassandra read/write latency
    try:
        f_stats.write(", CassandraDB")
        time_cur_db=int(time.time())
        tmp=commands.getoutput('nodetool cfstats cos')
        tmp1=tmp.replace("\t","").replace(":","").replace(" ms.","ms")
        cfstas_line=tmp1.split('\n')
        f_stats.write(" "+ str(cfstas_line[2]))
        f_stats.write(" "+ str(cfstas_line[4]))
    except IndexError:
        f_stats.write(" unavailable")
        pass
    except:
        print "Unexpected error:"
        pass

    # 6. Calucate read/write rate 
    try:
        cfstas_field=tmp1.replace("\n"," ").split(' ')
        index=cfstas_field.index('Count')
        read_count_cur=int(cfstas_field[index+1])
        write_count_cur=int(cfstas_field[index+7])
    except IndexError:
        pass
    except ValueError:
        pass
    except:
        print "Unexpected error:"
        pass
    
    if i > 1 :
        try:
          time_diff=time_cur_db - time_pre_db      
          f_stats.write(" ReadRate " + str((read_count_cur - read_count_pre) / time_diff))
          f_stats.write(" WriteRate " + str((write_count_cur - write_count_pre) / time_diff))
          time_pre_db=time_cur_db
          read_count_pre=read_count_cur
          write_count_pre=write_count_cur
        except IndexError:
          pass
        except ValueError:
          pass
        except:
          print "Unexpected error:"
          pass

    # 7. Retrieve DB size
    try:
        f_stats.write(", DB Size")
        tmp=commands.getoutput('du -d 0 /var/lib/cassandra/')
        tmp1=tmp.split('\t')
        db_size=float(int(tmp1[0])/ 1024 / 1024)
        f_stats.write(" "+ str(db_size))

        tmp=commands.getoutput('df -Th /var/lib/cassandra')
        db_percent=re.search(r'\d+%', tmp)
        f_stats.write(" Byte "+ str(db_percent.group(0)))
        #db_percent=tmp.split(' ')
        #f_stats.write(" "+ str(db_percent[25]))
        #f_stats.write(" Byte "+ str(db_percent[29]))  
    except IndexError:
        f_stats.write("Retrieve DB size: unavailable")
        pass
    except:
        print "Retrieve DB size - Unexpected error:"
        pass
        
    # 8. Retrieve NIC Rx/Tx and calucate transaction rate
    try: 
        time_cur_nic=float(time.time())
        nic1_tx_byte=int(commands.getoutput('cat /sys/class/net/enp130s0f0/statistics/tx_bytes'))
        nic1_rx_byte=int(commands.getoutput('cat /sys/class/net/enp130s0f0/statistics/rx_bytes'))
        
        if i > 1 :
            time_diff=time_cur_nic - time_pre_nic
            nic1_tx_rate=(nic1_tx_byte - nic1_tx_byte_pre) * 8 / time_diff
            nic1_rx_rate=(nic1_rx_byte - nic1_rx_byte_pre) * 8 / time_diff
            
            f_stats.write(", NIC enp130s0f0 TxRate " + str(nic1_tx_rate))
            f_stats.write("bps RxRate " + str(nic1_rx_rate))
            f_stats.write("bps")
       
        time_pre_nic=time_cur_nic
        nic1_tx_byte_pre=nic1_tx_byte
        nic1_rx_byte_pre=nic1_rx_byte
    except:
        print "Retrieve NIC Rx/Tx - Unexpected error:"
        pass

    # 8. Retrieve Key Num of Table Metadata 
    try:
        f_stats.write(", Metadata Key Num")
        tmp=commands.getoutput('nodetool cfstats cos|grep -A 8 "Table: metadata"|grep keys')
        metadata_keys=tmp.split(' ')
        f_stats.write(" "+ str(metadata_keys[4]))
    except IndexError:
        f_stats.write(" unavailable")
        pass
    except:
        print "Retrieve Metadata Key Num - Unexpected error:"
        pass

    f_stats.write("\n")
    i += 1
    f_stats.close()
    f_stats.close()
    time.sleep(interval)


