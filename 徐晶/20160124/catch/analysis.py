#!/usr/bin/env python
# -*- coding: utf8 -*-
#
# multiprocessing tshark to filter packet records
# 
import multiprocessing
import subprocess
from glob import glob
import os

def tshark_display_filter(cmd):
    """
    Filter packet records from cap files by tshark
    Input: tshark command
    Output: packet records
    """
    output = subprocess.Popen(cmd,
        stdout=subprocess.PIPE,
        stderr=None,
        )
    return output.stdout.read()

def start_process():
    # for logging 
    logging.info('Starting %s', multiprocessing.current_process().name)

if __name__ == "__main__":
    import logging
    logging.basicConfig(filename='analysis.log',level=logging.DEBUG)

    # Input files to be processed, MODIFY it to change input files
    #in_files = glob('*_20140610*')
    in_files = glob('dns/*')
    # Output file name, MODIFY it to change output file
    #out_file = 'dns.log/0610'
    out_file = 'new.dns'
    # output
    results = []


    # command example
    dns_cmd = """
    tshark -n -r packets_01529_20140618114903.cap \
    -T fields -E separator=|\
    -e dns.qry.name \
    -e dns.resp.ttl \
    -e dns.resp.addr
    """
    # cmd to list which is needed by function (pool.apply_async
    cmd = dns_cmd.split()
    
    # multiprocessing
    # set pool size, no bigger than input file number
    pool_size = min(multiprocessing.cpu_count()/2, len(in_files))

    logging.info('Pool size: %s', pool_size)
    pool = multiprocessing.Pool(processes=pool_size,
        initializer=start_process,
        )
    for fn in in_files:
        logging.info('processing %s', fn)
        # replace the input filename in cmd
        cmd[3] = fn
        logging.info('run "%s"', cmd)
        results.append(pool.apply_async(tshark_display_filter, (cmd, )))
     
    pool.close()
    pool.join()
    logging.info("Sub-process(es) done.")

    # results to file
    with open(out_file, 'w') as f:
        for result in results:
            f.write(result.get())
    for fn in in_files:
        os.remove(fn)

