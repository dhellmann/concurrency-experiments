#!/usr/bin/env python

import argparse
import sys
import time

import etcd
import urllib3

TRIES=3

parser = argparse.ArgumentParser('tenant owner behavior demo')
parser.add_argument('--owner', action='store_true', default=False)
parser.add_argument('-n', dest='num_messages', action='store', type=int,
                    default=10)
parser.add_argument('--ttl', action='store', type=int, default=10)
parser.add_argument('--timeout', action='store', type=int, default=2)
parser.add_argument('tenant', action='store')
parser.add_argument('clientid', action='store')
args = parser.parse_args(sys.argv[1:])

print 'tenant=%r' % args.tenant

client = etcd.Client(host='127.0.0.1', port=4001, allow_redirect=True)

# Calling get() on a lock modifies its internal state to change the
# index and value to the owner rather than what we have set in our
# constructor. We use separate lock instances to protect against that.
lock_name = '/%s' % args.tenant
my_lock = client.get_lock(lock_name, ttl=args.ttl, value=args.clientid)
poll_lock = client.get_lock(lock_name, ttl=args.ttl, value=args.clientid)

def show_lock_owner(lock):
    try:
        if lock.is_locked():
            l = lock.get()
            return 'locked by %r (%s)' % (lock.value, lock._index)
        else:
            return 'lock is not locked'
    except etcd.EtcdException as e:
        if 'Key not found' not in str(e) and 'non-existent' not in str(e):
            raise


def safe_release(lock):
    """Release a lock, ignoring 'does not exist' errors.
    """
    try:
        lock.release()
    except etcd.EtcdException as e:
        # If another client unlocks the lock using the same id
        # we use to lock it, then we will get an error when we
        # try to unlock it.
        if 'Key not found' not in str(e):
            raise


def safe_acquire(lock):
    try:
        my_lock.acquire(args.timeout)
    except urllib3.exceptions.TimeoutError:
        return False
    # Sometimes the lock call does not raise a timeout exception so we
    # have to check explicitly
    poll_lock.get()
    return (poll_lock.value == args.clientid)

###

print 'before:', show_lock_owner(poll_lock)

if args.owner:
    # Acquire the lock on startup
    for i in range(TRIES):
        print 'trying to acquire lock on owned tenant'
        if safe_acquire(my_lock):
            print 'got it'
            break
        print 'waiting to try again'
        time.sleep(2)
    else:
        raise RuntimeError('Never acquired lock for tenant we own')

messages = range(args.num_messages)

try:
    for message in messages:
        # Pretend to try to do some work with the tenant's router because
        # a message has arrived
        print 'message: %s' % message,
        if safe_acquire(my_lock):
            print 'PROCESSING', show_lock_owner(poll_lock)
            my_lock.renew(args.ttl)
            time.sleep(1)
            if not args.owner:
                # We are not the owner, so we do not want to hold the lock
                # any longer than needed to process this message.
                print 'RELEASING UNOWNED LOCK'
                safe_release(my_lock)
        else:
            print 'SKIPPING', show_lock_owner(poll_lock)
finally:
    # If we're exiting as the owner, release the lock we have so someone
    # else can take over managing the tenant.
    if args.owner:
        print 'RELEASING OWNED LOCK'
        safe_release(my_lock)
