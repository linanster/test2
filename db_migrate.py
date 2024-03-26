import threading
import random
import time
import pymysql
import queue
from threading import Thread
from threading import Event
#


# src database
src_host = 'rm-j6c728808eow1mob85o.mysql.rds.aliyuncs.com'
src_port = 3306
src_user = 'root'
src_passwd = 'PasswordTest!'
src_db = 'db_src'
src_fields = ('name', 'age')
src_table = 'stu'
# dest database
dest_host = 'rm-j6c728808eow1mob85o.mysql.rds.aliyuncs.com'
dest_port = 3306
dest_user = 'root'
dest_passwd = 'PasswordTest!'
dest_db = 'db_dest'
dest_fields = ('name', 'age')
dest_table = 'stu'

def mytime():
    return time.strftime('%H:%M:%S', time.localtime())
def mythread():
    return threading.current_thread().name

class Conn:
    def __init__(self, name, host, port, user, passwd, db):
        self.myname = name
        self.conn = None
        try:
            self.conn = pymysql.Connect(host=host, port=port, user=user, passwd=passwd, db=db)
            print('db conn {} established'.format(self.myname))
        except Exception as e:
            print('db conn {} establish error'.format(self.myname))
            print(e)
    def __repr__(self):
        return self.myname
    def __str__(self):
        return self.myname
    # def __del__(self):
    #     try:
    #         self.conn.close()
    #         print('db conn {} destroyed'.format(self.name))
    #     except NameError:
    #         pass

class ConnPool:
    def __init__(self, name, count, host, port, user, passwd, db):
        self.name = name
        self.count = count
        self.sem = threading.Semaphore(count)
        self.pool = queue.Queue()
        for i in range(count):
            self.pool.put(Conn('{}-conn-{}'.format(self.name, i+1), host, port, user, passwd, db))
    def get_conn(self):
        self.sem.acquire()
        return self.pool.get()
    def return_conn(self, conn):
        self.pool.put(conn)
        self.sem.release()
    def __del__(self):
        while not self.pool.empty():
            myconn = self.pool.get()
            try:
                myconn.conn.close()
                print('db conn {} closed'.format(myconn))
            except Exception as e:
                print('db conn {} close error'.format(myconn))
                print(e)
        print('ConnPool {} destroyed'.format(self.name))

def work(pool):
    print('{} | {} | start'.format(mytime(), mythread()))
    conn = pool.get_conn()
    print('{} | {} | get {}'.format(mytime(), mythread(), conn))
    threading.Event().wait(random.randint(1,3))
    pool.return_conn(conn)
    print('{} | {} | done and return {}'.format(mytime(), mythread(), conn))

def gen_insert_sql(table, fields, record):
    fields_pattern = ','.join(['{}' for _ in range(len(fields))])
    sql_insert_pattern = 'insert into {} ({}) values {}'.format('{}', fields_pattern, '{}')
    # print('==pattern==', sql_insert_pattern)
    return sql_insert_pattern.format(table, *fields, record)

# print(gen_insert_sql('stu', src_fields, ('nan', 32)))
# input()

class Task:
    def __init__(self, src_record, dest_pool, dest_fields, dest_table):
        self.src_record = src_record
        self.dest_pool = dest_pool
        self.dest_fields = dest_fields
        self.dest_table = dest_table
    def run(self):
        myconn = dest_pool.get_conn()
        dest_conn = myconn.conn
        dest_cursor = dest_conn.cursor()
        # todo: check if exist in dest table
        # sql_insert_str = 'insert into {} ({},{}) values (\"{}\",{})'.format(self.dest_table, *self.dest_fields, *self.src_record)
        sql_insert_str = gen_insert_sql(self.dest_table, self.dest_fields, self.src_record)
        print('==sql_insert_strl==', sql_insert_str)
        try:
            dest_cursor.execute(sql_insert_str)
            dest_conn.commit() # todo: batch
        except Exception as e:
            print('==mysql execute error==')
            print(e)
        finally:
            print('==return conn {}=='.format(myconn))
            dest_pool.return_conn(myconn)

class MyThread(Thread):
    def __init__(self, work_queue, e):
        super().__init__()
        self.work_queue = work_queue
        self.e = e
        print('{} | init'.format(self.getName()))
    def run(self):
        self.e.wait()
        while True:
            task = self.work_queue.get()
            if task == -1:
                break
            try:
                task.run()
            except Exception as e:
                print('==MyThread run error==')
                print(e)
    def __del__(self):
        print('{} | del'.format(self.getName()))

class MyThreadPool:
    def __init__(self, num):
        self.num = num
        self.work_queue = queue.Queue()
        self.e = Event()
        for _ in range(num):
            MyThread(self.work_queue, self.e).start()
    def add_task(self, task):
        self.work_queue.put(task)
    def start(self):
        self.e.set()
    def close(self):
        for _ in range(self.num):
            self.work_queue.put(-1)

def gen_select_sql(fields, table):
    fields_pattern = ','.join(['{}' for _ in range(len(fields))])
    # sql_select_pattern = 'select' + ' ' + fields_pattern + ' ' + 'from' + ' ' + '{}'
    sql_select_pattern = 'select {} from {}'.format(fields_pattern, table)
    return sql_select_pattern.format(*fields, table)
    
    

class SrcUtils:
    def __init__(self):
        self.src_pool = ConnPool('src', 1, src_host, src_port, src_user, src_passwd, src_db)
        self.src_myconn = self.src_pool.get_conn()
        self.src_cursor = self.src_myconn.conn.cursor()
        sql_select_str = gen_select_sql(src_fields, src_table)
        print('==sql_select_str==', sql_select_str)
        self.src_cursor.execute(sql_select_str)
    def fetchone(self):
        return self.src_cursor.fetchone()
    def close(self):
        self.src_pool.return_conn(self.src_myconn)
        print('==return conn {}=='.format(self.src_myconn))


if __name__ == '__main__':

    src_utils = SrcUtils()
    dest_pool = ConnPool('dest', 3, dest_host, dest_port, dest_user, dest_passwd, dest_db)
    input()

    thread_pool = MyThreadPool(3)
    thread_pool.start()

    while True:
        src_record = src_utils.fetchone()
        print('==src_recrod==', src_record)
        if src_record is None:
            thread_pool.close()
            break
        else:
            thread_pool.add_task(Task(src_record, dest_pool, dest_fields, dest_table))
            # todo: low speed
    src_utils.close()
    print('{} | end'.format(mythread()))
