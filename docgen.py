#!/usr/bin/python

from tabulate import tabulate
from jinja2 import Environment, DictLoader

import requests
import sys
import time
import random
import json
import os

def error_message(msg):
    print(msg)
    raise Exception(msg)

"""
def message_template1(es_doc_id):
    t = {
         '~elementid': 'Id'+str(es_doc_id), 
         '~entrystatus': "new",
         '~label' : "Interface",
         '~modificationtime' : int(round(time.time() * 1000)),
         '~nextmodificationtime' : int(round(time.time() * 1000)),
         'wqePoolScore' : random.random()
        }  
    return t 


def message_template2(es_doc_id):
    t = {
         '~elementid': 'Id'+str(es_doc_id), 
         '~entrystatus': "new",
         '~label' : "NwDevice",
         '~modificationtime' : int(round(time.time() * 1000)),
         '~nextmodificationtime' : int(round(time.time() * 1000)),
         'param1' : random.random()
        }  
    return t     

es_templates = {'doctest1' : message_template1, 'doctest2': message_template2}

def get_message_from_template(template, es_doc_id):
    m = es_templates[template](es_doc_id)
    return m
"""

def print_progress(msg):
    print ("\r "+msg, end="")


def get_doc_count(index_name, p):
    count = 0 
    stats_url = p["es_url"]+index_name +'/_stats/docs'
    r = requests.get(stats_url)
    if (r.status_code == requests.codes.ok):
        s = r.json()
        #print (s)
        count = s["indices"][index_name]["total"]["docs"]["count"]
    return count

def read_profile_dir_file(p, file):
    
    file_name = os.path.join(p["profile_dir"], file)
    with open(file_name) as f:
        return f.read()

def load_templates(index_name, p):
    
    #load all document templates for the index whose name starting with <index_name>-doc-template*
    template_dict = {}
    prefix = index_name+'-doc-template'
    files = os.listdir(p["profile_dir"])
    for file in files:
        if file.startswith(prefix):
            template_dict[file] = read_profile_dir_file(p, file)

    template_env = Environment(loader = DictLoader(template_dict))        
    p[prefix] = template_env
    return

"""
Picks a random template, adds the context and evaluates it to document payload
"""
def get_message_from_template(index_name, p, doc_index, doc_count, doc_id):
    env = p[index_name+'-doc-template']
    ctxt = {
            "doc_index" : doc_index,
            "doc_id"    : doc_id,
            "doc_id_str": 'Id'+str(doc_id),
            "doc_count" : doc_count,
            "index_name": index_name,
            "rand_value": random.random(),
            "time_now" : int(round(time.time() * 1000))   
    }
    
    #pick a random template for this index 
    template_name = random.choice(list(env.loader.mapping))
    template = env.get_template(template_name)
    payload = template.render(ctxt)
    return payload


def add_documents(index_name, p):
    es_doc_type = p["doc_type"]
    es_doc_count = p["doc_count"]
    

    es_doc_id = get_doc_count(index_name, p) +1
    dcount = 0 
    errors = 0
    for doc_index in range(es_doc_id, es_doc_id+es_doc_count):
        dcount = dcount+1
        doc = get_message_from_template(index_name, p, dcount, es_doc_count, doc_index)

        doc_url = p["es_url"]+index_name+'/'+es_doc_type+'/'

        #if this is the last document, force a refresh
        if (doc_index == es_doc_count+es_doc_id-1):
            doc_url = doc_url +'?refresh=wait_for'

        print_progress ('Index:'+ index_name+' Adding documents: '+str(dcount)+' of '+str(es_doc_count)+' (errors:'+str(errors)+')')
        r = requests.post(doc_url, data=doc, headers = {'content-type' :'application/json'})
        if (r.status_code not in [200, 201]):
            errors = errors + 1
            print (doc_url)
            print (doc)
            print (r.json)
            r.raise_for_status()

    print("")
        
def gen_one_document(index_name, p):
    es_doc_type = p["doc_type"]
    es_doc_count = p["doc_count"]
    
    es_doc_id = get_doc_count(index_name, p) +1
    dcount = 0 
    for doc_index in range(es_doc_id, es_doc_id+es_doc_count):
        dcount = dcount+1
        doc = get_message_from_template(index_name, p, dcount, es_doc_count, doc_index)

        #generate one doc and come out
        print (doc)
        break

    print("")

def setup_index(index_name, p, fail_if_exists):
    index_url = p["es_url"]+index_name
    r = requests.get(index_url)
    if (r.status_code != 404) :
        if (fail_if_exists):
            error_message ("Index:"+index_name+" already exists. Use docgen reset and try again")
    else:        
        #create index & type mappings
        r = requests.put(index_url, data=read_profile_dir_file(p, index_name+"-index.json"))
        if r.status_code != requests.codes.ok:
            print ("Error creating index: "+ index_name)
            r.raise_for_status()



def dump_index_stats(p):
    #get docs count and index size stats
    stats = []
    for index_name in p["indices"]:
        stats_url = p["es_url"]+index_name +'/_stats/docs,store'
        r = requests.get(stats_url)
        if (r.status_code == requests.codes.ok):
            s = r.json()
            #print (s)
            itotals = s["indices"][index_name]["total"]
            isize = itotals["store"]["size_in_bytes"]
            stats.append([index_name, itotals["docs"]["count"], isize, isize/(1024*1024) ])
        else:
            r.raise_for_status()

    print (tabulate(stats, ["Index", "Count", "Index Size (Bytes)", "Index Size (MB)"]))


def load_profile(profile_dir):
    p = None
    profile_file = os.path.join(profile_dir, 'profile.json')
    if (not os.path.exists(profile_file)):
        error_message("Error: Either profile directory doesn't exit or profile.json is missing")

    with open(profile_file) as profile_data:
        p = json.load(profile_data)

    if (not p["es_url"].endswith('/')):
        p['es_url'] = p['es_url'] +'/'
    p['profile_dir'] = profile_dir

    #load all document templates
    for index_name in p["indices"]:  
        load_templates(index_name, p)
    return p

def reset_profile_cmd(p, args):
    for index_name in p["indices"]:    
        index_url = p["es_url"]+index_name
        r = requests.delete(index_url)
        try:
            r.raise_for_status()
        except:
            print("Unexpected error:", sys.exc_info())
            pass
            

def run_profile_cmd(p, args):

    if len(args) > 0:
        print (" Overriding document count to add with:"+args[0])
        p["doc_count"] = int(args[0])


    for index_name in p["indices"]:  
        setup_index(index_name, p, False)
        add_documents(index_name, p)

    dump_index_stats(p)

def init_profile_cmd(p, args):
    for index_name in p["indices"]:  
        setup_index(index_name, p, True)


def stats_profile_cmd(p, args):
    dump_index_stats(p)

def generatedoc_profile_cmd(p, args):
    for index_name in p["indices"]:  
        gen_one_document(index_name, p)

def run_command(argv):
    
    if (len(argv) < 3):
        print("usage: ", argv[0], "<profile diretory>", "init|run|reset|stats|gendoc")
        print("  <profile directory> = directory containing profile.json and other index and data templates")
        print("  init = one time initialization of indices defined in the profile ")
        print("  run =  add documents based on the mappings and templates. Args: Doc count ")
        print("  reset = delete indices specified in the profile ")
        print("  stats = Dump statistics ")
        print("  gendoc = Test: Generate one doc per index and get out ")
        
        return

    profile_dir = argv[1]
    profile_cmd = argv[2]
    
    p = load_profile(sys.argv[1])

    cmd_args = argv[3:]
    if ("run" == profile_cmd):
        run_profile_cmd(p, cmd_args)
    elif ("init" == profile_cmd):
        init_profile_cmd(p, cmd_args)
    elif ("reset" == profile_cmd):
        reset_profile_cmd(p, cmd_args)
    elif ("stats" == profile_cmd):
        stats_profile_cmd(p, cmd_args)
    elif ("gendoc" == profile_cmd):
        generatedoc_profile_cmd(p, cmd_args)
    else:
        print("Unknown command: "+profile_cmd)
    return

run_command(sys.argv)
