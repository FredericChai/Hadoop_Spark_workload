#!/usr/bin/python

import sys


def read_map_output(file):
    """ Return an iterator for key, value pair extracted from file (sys.stdin).
    the record has already been sorted by hadoop streambing according to c_id
    Input format:  key \t value -> c_id \t country \t v_id
    Output format: (c_id, v_id,country)
    """
    for line in file:
        yield line.strip().split('\t')


def reducer():
    """
    Input format: tag \t owner
    Output format:
                country:
                category \t total:count \t percent in another country
    """
    current_c_id=''
    c1={} #use c1 refers to country one
    c2={}
    total=0
    percent=0.0
    sharev=0
    country=''
    for line in read_map_output(sys.stdin):

        if len(line)<=1:
            continue
        c_id,v_id,country = line[0],line[1],line[2]
        # print(country)
        if current_c_id !=c_id:
            if current_c_id !='': 
                total = len(c1)
                sharev = len(set(c1).intersection(set(c2)))
                percent = sharev*100/len(c2)
                print("category:{}; total:{};{} same with US,{} in US".format(current_c_id,total,sharev,percent))

        if country=='ca':
            c1[v_id]=0
        if country=='us':
            c2[v_id]=0       
        current_c_id = c_id
    
    if current_c_id !='':
        total = len(c1)
        sharev = len(set(c1).intersection(set(c2)))
        percent = sharev*100/len(c2)
        print("category:{}; total:{};{} same with US,{} in US".format(current_c_id,total,sharev,percent))
        
                
if __name__ == "__main__":
    reducer()
