#!/usr/bin/env python3

from time import time
from datetime import datetime, timedelta
from glob import glob
import re
import os

backup_dir = '/opt/backups/'
backup_prefix = 'db_backup_'
backup_suffix = '.pg.dump'
file_age_limit = timedelta(days=31)
file_count_limit = 100

backup_files = dict()


def create_new_backup():
    now = '{}'.format(time())

    filename = backup_dir + backup_prefix + now + backup_suffix
    with open(filename, 'w') as f:
        f.write('Sample data\n')


def get_all_backups():
    filenames = glob(backup_dir + backup_prefix + "*" + backup_suffix)
    for filename in filenames:
        pattern = '.*' + backup_prefix + '(\d+\.?\d+)' + backup_suffix
        prog = re.compile(pattern)
        result = prog.match(filename)
        timestamp = result.group(1)
        timestamp = datetime.utcfromtimestamp(float(timestamp))

        if filename not in backup_files:
            backup_files[filename] = timestamp


def remove_old_backups():
    num_files = len(backup_files)
    for filename in sorted(backup_files):
        timestamp = backup_files[filename]
        now = datetime.utcnow()
        age = now - timestamp
        if age >= file_age_limit:
            print('Removing file [{}] older [{}] than limit [{}]'.format(filename, age, file_age_limit))
            os.remove(filename)
            num_files -= 1
        elif num_files > file_count_limit:
            print('Removing file [{}] due to quantity limit [{}]'.format(filename, file_count_limit))
            os.remove(filename)
            num_files -= 1


def main():
    create_new_backup()
    get_all_backups()
    remove_old_backups()


if __name__ == '__main__':
    main()
