#!/usr/bin/env python

import argparse
import json
import logging
import os
import subprocess

try:
    from urlparse import urlparse
except ImportError:
    from urllib.parse import urlparse

try:
    from urllib2 import Request, urlopen
except ImportError:
    from urllib.request import Request, urlopen


TSURU_TARGET = os.environ['TSURU_TARGET']
TSURU_TOKEN = os.environ['TSURU_TOKEN']


def main():
    logging.basicConfig(level=logging.INFO, format='%(levelname)s %(message)s')
    parser = argparse.ArgumentParser(
        description='Get healthcheck for all units'
    )
    parser.add_argument('-a', metavar='app', type=str,
                        help='Name of app')

    args = parser.parse_args()
    dbaas_shell(args.a)


def dbaas_shell(app):
    url = '%s/apps/%s/env' % (TSURU_TARGET, app)
    req = Request(url, None, {'Authorization': TSURU_TOKEN})

    try:
        resp = urlopen(req, timeout=3)
    except Exception as err:
        logging.exception(err)
        return

    if resp.code != 200:
        logging.error('Failed to get environment variables: %d', resp.code)
        return

    data = json.loads(resp.read().decode('utf-8'))
    env = {i['name']: i['value'] for i in data}
    dbs = list(discover_dbs(env))

    print('Which database do you want to open shell: ')
    for i, db in enumerate(dbs):
        print('%d - %s' % (i, db['name']))

    pos = int(input())
    open_shell(dbs[pos])


def discover_dbs(env):
    tsuru_services = json.loads(env.get('TSURU_SERVICES', '{}'))

    for instance in discover_instances(tsuru_services):
        name = instance['instance_name']
        envs = instance['envs']

        if 'DBAAS_SENTINEL_ENDPOINT' in envs:
            for db in discover_redis_sentinel_hosts(name, envs['DBAAS_SENTINEL_ENDPOINT']):
                yield db

        elif 'DBAAS_MONGODB_ENDPOINT' in envs:
            for db in discover_mongodb_hosts(name, envs['DBAAS_MONGODB_ENDPOINT']):
                yield db

        elif 'DBAAS_REDIS_ENDPOINT' in envs:
            yield discover_single_redis(name, envs['DBAAS_REDIS_ENDPOINT'])

        elif 'DBAAS_MYSQL_ENDPOINT' in envs:
            yield discover_single_mysql(name, envs)


def discover_instances(tsuru_services):
    possible_services = ['tsuru-dbaas', 'tsuru-dbaas-dev', 'tsuru-dbaas-qa2']

    for service in possible_services:
        for item in tsuru_services.get(service, []):
            yield item


def discover_redis_sentinel_hosts(name, sentinel_endpoint):
    url = urlparse(sentinel_endpoint)
    hosts = url.netloc.split('@', 1)[1].split(',')

    for host in hosts:
        pair = host.split(':', 1)
        hostname = pair[0]

        yield {
            'name': 'redis: %s via %s' % (name, hostname),
            'type': 'redis',
            'password': url.password,
            'hostname': hostname,
        }


def discover_single_redis(name, endpoint):
    url = urlparse(endpoint)
    host = url.netloc.split('@', 1)[1]
    pair = host.split(':', 1)
    hostname = pair[0]

    return {
        'name': 'redis: %s via %s' % (name, hostname),
        'type': 'redis',
        'password': url.password,
        'hostname': hostname,
    }

def discover_mongodb_hosts(name, endpoint):
    url = urlparse(endpoint)
    hosts = url.netloc.split('@', 1)[1].split(',')

    for host in hosts:
        yield {
            'name': 'mongodb: %s via %s' % (name, host),
            'type': 'mongo',
            'password': url.password,
            'username': url.username,
            'path': url.path,
            'hostname': host,
        }


def discover_single_mysql(name, env):
    url = urlparse(env['DBAAS_MYSQL_ENDPOINT'])
    userhostparts = url.netloc.split('@', 1)
    userpassword = userhostparts[0]
    user, password = userpassword.split(':', 1)
    host = userhostparts[1]
    pair = host.split(':', 1)
    hostname = pair[0]
    database = url.path.lstrip('/')
    host = env['DBAAS_MYSQL_HOSTS']
    return {
        'name': 'mysql: %s via %s' % (name, hostname),
        'type': 'mysql',
        'username': user,
        'password': password,
        'hostname': hostname,
        'database': database,
    }


def open_shell(db):
    if db['type'] == 'redis':
        args = [
            'redis-cli',
            '-h',
            db['hostname'],
            '-a',
            db['password']
        ]

    elif db['type'] == 'mongo':
        args = [
            'mongo',
            '%s%s' % (db['hostname'], db['path']),
            '-u',
            db['username'],
            '-p',
            db['password'],
        ]

    elif db['type'] == 'mysql':
        args = [
            'mysql',
            '-h',
            db['hostname'],
            '-u',
            db['username'],
            '-p',
            db['password'],
            db['database'],
        ]


    print(' '.join(args))
    subprocess.call(args)


if __name__ == '__main__':
    main()
