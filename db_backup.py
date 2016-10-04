#!/usr/bin/env python3

from time import time
from datetime import datetime, timedelta
from glob import glob
import os, sys, re, toml, pexpect, shutil


db_params_toml = './db_params.toml'
backup_dir_prefix = '/opt/backups/db-backup_'
backup_age_limit = timedelta(days=31)
backup_count_limit = 100

backup_files = dict()
db = None


def read_db_params():
    global db
    with open(db_params_toml, 'r') as f:
        db = toml.loads(f.read())
        db = db['database']


def create_new_backup():
    now = '{}'.format(time())
    for schema in db['schemas']:
        backup_dir = backup_dir_prefix + now + '/'
        filename_prefix = backup_dir + schema + '.'

        if not os.path.exists(backup_dir):
            os.makedirs(backup_dir)

        sql_backup = 'pg_dump -n {} -s -h {} -d {} -U {} -W -f {}'.format( \
            schema, db['host'], db['database'], db['username'], filename_prefix + 'sql')
        # print(sql_backup)

        full_backup = 'pg_dump -n {} -h {} -d {} -U {} -W -f {}'.format( \
            schema, db['host'], db['database'], db['username'], filename_prefix + 'dump')
        # print(full_backup)

        child = pexpect.spawn('%s'%(sql_backup))
        i = child.expect([pexpect.TIMEOUT, '[Pp]assword: '])
        if i == 0: # Timeout
            print('ERROR!')
            print('pg_dump startup failed. Here is what pg_dump said:')
            print(child.before, child.after)
            sys.exit (1)
        child.sendline(db['password'])

        child = pexpect.spawn('%s'%(full_backup))
        i = child.expect([pexpect.TIMEOUT, '[Pp]assword: '])
        if i == 0: # Timeout
            print('ERROR!')
            print('pg_dump startup failed. Here is what pg_dump said:')
            print(child.before, child.after)
            sys.exit (1)
        child.sendline(db['password'])


def get_all_backups():
    backup_dirs = glob(backup_dir_prefix + "*" + '/')
    for backup_dir in backup_dirs:
        pattern = '.*' + backup_dir_prefix + '(\d+\.?\d+)' + '/'
        prog = re.compile(pattern)
        result = prog.match(backup_dir)
        timestamp = result.group(1)
        timestamp = datetime.utcfromtimestamp(float(timestamp))

        if backup_dir not in backup_files:
            backup_files[backup_dir] = timestamp


def remove_old_backups():
    num_backups = len(backup_files)
    for backup_dir in sorted(backup_files):
        timestamp = backup_files[backup_dir]
        now = datetime.utcnow()
        age = now - timestamp
        if age >= backup_age_limit:
            print('Removing dir [{}] older [{}] than limit [{}]'.format(backup_dir, age, backup_age_limit))
            shutil.rmtree(backup_dir)
            num_backups -= 1
        elif num_backups > backup_count_limit:
            print('Removing dir [{}] due to quantity limit [{}]'.format(backup_dir, backup_count_limit))
            shutil.rmtree(backup_dir)
            num_backups -= 1


def main():
    read_db_params()
    create_new_backup()
    get_all_backups()
    remove_old_backups()


if __name__ == '__main__':
    main()
