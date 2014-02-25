#!/usr/bin/env python

import sys
import time

import etcd
import urllib3

if len(sys.argv) <= 1:
    raise ValueError('Please specify the client id')
clientid=sys.argv[-1]
print 'clientid=%r' % clientid

client = etcd.Client(host='127.0.0.1', port=4001, allow_redirect=True)

# Calling get() on a lock modifies its internal state to change the
# index and value to the owner rather than what we have set in our
# constructor. We use separate lock instances to protect against that.
my_lock = client.get_lock('/test1', ttl=10, value=clientid)
poll_lock = client.get_lock('/test1', ttl=10, value=clientid)

def show_lock_owner(lock):
    if lock.is_locked():
        try:
            l = lock.get()
        except etcd.EtcdException as e:
            if 'non-existent' not in str(e):
                raise
        else:
            return 'already locked by %r (%s)' % (lock.value, lock._index)
    else:
        return 'lock is not locked'

print 'before:', show_lock_owner(poll_lock)

for i in range(5):
    got_it = False
    print 'acquiring'
    try:
        my_lock.acquire(2)
        got_it = True
    except urllib3.exceptions.TimeoutError:
        # Raw timeout error from the communications library
        # not wrapped by etcd error
        print 'Could not acquire the lock', show_lock_owner(poll_lock)
        continue
    else:
        # Renew the lock a few times. If another client has released
        # it the renewal may fail.
        for i in range(5):
            print 'during:', show_lock_owner(poll_lock)
            try:
                my_lock.renew(5)
            except etcd.EtcdException as e:
                if 'Key not found' not in str(e):
                    raise
            time.sleep(1)
    finally:
        # FIXME: Need some error handling here, but it's not clear why
        # the exception isn't being converted to a type we can catch.
        if got_it:
            try:
                my_lock.release()
            except etcd.EtcdException as e:
                # If another client unlocks the lock using the same id
                # we use to lock it, then we will get an error when we
                # try to unlock it.
                if 'Key not found' not in str(e):
                    raise

print 'after:', my_lock.is_locked()
