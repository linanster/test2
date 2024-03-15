import random
import queue
import requests
import json
from time import time
from threading import Thread, Event
from datetime import datetime
#
# def mytime():
#     return time.strftime('%H:%M:%S', time.localtime())

class MyThread(Thread):
    def __init__(self, work_queue, e):
        super().__init__()
        self.work_queue = work_queue
        self.e = e
        self.time = lambda : datetime.now().strftime('%H:%M:%S')
        # print('{} | Thread {} | init'.format(mytime(), self.name))
        print('Thread {} | init'.format(self.name))
    def run(self):
        self.e.wait()
        while True:
            if self.work_queue.empty():
                break
            func, args = self.work_queue.get()
            func(*args, thread_name=self.name)

class MyPool:
    def __init__(self, num):
        self.work_queue = queue.Queue()
        self.e = Event()
        for _ in range(num):
            MyThread(self.work_queue, self.e).start()
    def add_job(self, job):
        self.work_queue.put(job)
    def start(self):
        self.e.set()

def func(*args, thread_name=None):
    url = "https://dev-gateway.on-running.cn/graphql"
    
    # 1.addressOptions
    # payload="{\"query\":\"query AddressOptions {\\n    addressOptions {\\n        id\\n        type\\n        name\\n    }\\n}\\n\",\"variables\":{}}"
    # 2.memeberCoupons
    # payload = "{\"query\":\"query MemberCoupons {\\n    memberCoupons {\\n        count\\n    }\\n}\\n\",\"variables\":{}}"
    # 3.member
    payload = "{\"query\":\"query Member {\\n    member {\\n        id\\n        username\\n        cellphone\\n        memberUid\\n        headImage\\n        comment\\n        areaCode\\n        registerChannel\\n        registerDate\\n        name\\n        title\\n        email\\n        referedSource\\n        wechatNick\\n        wechatUnionId\\n        wechatOpenId\\n        level\\n        point\\n        storeCreditBalance\\n        growth\\n        gender\\n        birthday\\n        allowedChangeBirthday\\n        preferedSize\\n        height\\n        weight\\n        province\\n        city\\n        district\\n        street\\n        description\\n        memberQrcode\\n        groupId\\n    }\\n}\\n\",\"variables\":{}}"
    headers = {
       'channel': 'WXMP',
       'Content-Type': 'application/json',
       'Authorization': '7f937e39-94ff-4255-9283-4a0408165eb4',
    }
    start = time()
    response = requests.request("POST", url, headers=headers, data=payload)
    end = time()
    print('{} | {}'.format(thread_name, int((end-start)*1000)))


if __name__ == '__main__':

    pool = MyPool(40)
    for i in range(40*10):
        pool.add_job((func, (i,)))
    input("Start:")
    pool.start()
