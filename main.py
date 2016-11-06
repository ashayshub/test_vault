#!/usr/bin/env python
# Author: Ashay Chitnis
# Date: 29.10.2016
# Program to test multithreaded secret handling with vault
# Imp: Though psyscopg2 itself is threadsafe, cursors from psyscopg2 are not

import time
import random
import os
import threading
import logging
import argparse
import psycopg2
import queue
import hvac

class MyThread (threading.Thread):
    def __init__(self, threadID, name, func, token):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.name = name
        self.func = func
        self.token = token

    def run(self):
        logging.warning ("Starting " + self.name + '\n')
        self.func(self.name, self.token)
        logging.warning ("Exiting " + self.name)

def refresh_dbconn(tname, vault_token):
    """function to connect to db and refresh the details every 15 sec"""
    # get DB conn

    vault_url = 'http://localhost:8200'
    client = hvac.Client(url=vault_url, token=vault_token)
    while True:
        with u:
            logging.warning('{0}:- Db thread Waiting on worker threads'.format(tname))
            ret = u.wait()
            if ret:
                with v:
                    get_secret_backend(tname, client)
                    ret = get_dbconn(tname)
                    if not ret:
                        with e:
                            logging.warning('{0}:- Putting db handle false on queue'.format(tname))
                            sys_q.put(False)
                    else:
                        logging.warning('{0}:- Notifying all threads'.format(tname))
                        v.notify_all()
                        
def get_dbconn(tname):
   """ Connects to db and returns db connection """
   global conn
   connect_str = "dbname='{0}' user='{1}' host='{2}' password='{3}'".format(
                  arg.db_name, db_user, arg.db_host, db_pass)
   print('{0}:- Using DBuser: {1}, DBpass: {2}'.format(tname, db_user, db_pass))

   try:
       conn = psycopg2.connect(connect_str)
   except Exception as ex:
       logging.error('{0}:- Could not connect to the DB string. Exception: {1}'.format(tname, ex))
       return False
   else:
       print('{0}:- Got db handle'.format(tname))
       return conn

def get_secret_backend(tname, client):
    """Get secret creds for psql db"""

    global db_user, db_pass
    cred = client.read('postgresql/creds/readonly')
    db_user = cred['data']['username']
    db_pass = cred['data']['password']
    return True

def get_userinfo(tname, token):
    """ Function to start threads that query db """
    
    thread_data = threading.local()
    thread_data.cursor = None
    thread_data.counter = 0

    while True:
        # Check if new db handle is available after certain frequency
        max_check_interval = 3500
        thread_data.counter += 1
        with v:
            # get new db handle
            if thread_data.counter % max_check_interval == 0:
                logging.warning('{0}:- Hollering for Db Refresh'.format(tname))
                with u:
                    u.notify_all()
                logging.warning('{0}:- Worker thread wait reached'.format(tname))
                ret = v.wait()
                if ret:
                    print('{0}:- Using DBuser: {1}, DBpass: {2}'.format(tname, db_user, db_pass))
                    thread_data.cursor = refresh_cursor(tname, thread_data.cursor)

        #time.sleep(random.uniform(1,3))
        get_someuser(thread_data.cursor, tname)
            
def refresh_cursor(tname, cursor):
    """ Refresh Cursor"""
    if not conn:
        logging.warning('{0}:- No Db connn here'.format(tname))
        return None
    logging.warning('{0}:- Refreshing cursor'.format(tname))
    cursor = conn.cursor()
    return cursor
        
def get_someuser(cursor, tname):
   """ Gets some random user and from Db """
   if not cursor:
       logging.warning('{0}:- No cursor here..'.format(tname))
       return False
   sql = 'SELECT uname FROM test_users WHERE uname=\'ashay\' LIMIT 1';
   try:
       cursor.execute(sql)
   except Exception as ex:
       logging.warning ('{0}:- SQL: {1}'.format(tname, sql))
       logging.error('{0}:- Could not execite the statement. Exception: {1}'.format(tname, ex))
       return False
   else:
       print('{0}:- Got User: {1}\n'.format(tname, cursor.fetchone()[0]))
       return True

def parse_arguments():
   """ Parse input args """

   parser = argparse.ArgumentParser(description='Vault Multithread secret handling')
   parser.add_argument('--db-host', '-l', required=True, help="db host")
   parser.add_argument('--db-name', '-n', required=True, help="db name")
   parser.add_argument('--vault-token', '-t', required=True, help="vault token id")
   return parser.parse_args()

def main():
    """ Main func """
    
    global conn, arg, sys_q, e, v, u

    conn = None
    arg = parse_arguments()
    sys_q = queue.Queue()

    #lock for looking into the db connection error.
    e = threading.RLock()
    
    # Condition to wake up all threads from db threads
    # when the db thread is ready with the connection handle
    v = threading.Condition()

    # Condition to wake up db thread from other threads 
    # when they want to refresh the connection handle
    u = threading.Condition()

    #thread_names = ('T1',)
    thread_names = ('T1', 'T2', 'T3', 'T4')

    threads = list()

    # start threads

    # db thread
    db_thread = MyThread(77, 'DB', refresh_dbconn, arg.vault_token)
    db_thread.start()

    # worker thread
    for i, name in enumerate(thread_names):
       threads.append(MyThread(i, name, get_userinfo, None))
       threads[i].start()

    # exit on wrong db creds
    # ToDo handle exit more gracefuly. 

    while True:
        time.sleep(2)
        logging.warning('checking sys queue')
        with e:
            if sys_q.get() is False: 
                logging.warning('DB Auth failed. Hence got "False" on sys queue. Exiting....')
                os._exit(1)

    # join threads
    db_thread.join()

    for i, name in enumerate(thread_names):
        threads[i].join()
    
if __name__ == '__main__':
    main()
