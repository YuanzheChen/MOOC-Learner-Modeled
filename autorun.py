# /usr/bin/env python
import sys
import argparse
import subprocess
import getpass
import os
import yaml
import MySQLdb
import tempfile

# This script serves as an entry-point to config and run MLM automatically
# to visualize the distribution of features populated by MLM.

MOOCDB_CONFIG_SUB_STRUCTURE = {
    "MOOCdb": {
        "database": None,
        "work_dir": None,
        "MLM_folder": None
    },
    "mysql": {
        "host": None,
        "password": None,
        "port": None,
        "user": None
    },
    "full_pipeline": {
        "MLM": None
    },
}


MLM_CONFIG_TEMPLATE = {
    "mysql": {
        "query_user": False,
        "query_password": False,
        "query_database": False,
        "database": None,
        "host": None,
        "user": None,
        "password": None,
        "port": None
    }
}


def dict_structure(d):
    # Extract out the structure of dict cfg to
    # compare with the legal structure
    if isinstance(d, dict):
        return {k: dict_structure(d[k]) for k in d}
    else:
        # Replace all non-dict values with None.
        return None


def db_config_check(sup_dict, sub_dict):
    return all(item in dict_structure(sup_dict).items()
               if not isinstance(item[1], dict)
               else db_config_check(sup_dict[item[0]], sub_dict[item[0]])
               for item in dict_structure(sub_dict).items())


def autorun():
    # Parse arguments
    parser = argparse.ArgumentParser('Auto-config and run MLM automatically visualize'
                                     'moocdb features and data.')
    parser.add_argument('-c', action="store", default=None, dest='db_config_path',
                        help='path to MOOCdb config file')
    parser.add_argument("-s", action="store", default='moocdb', dest='db', help='MySQL moocdb database get_redis_key')
    parser.add_argument("-t", action="store", default='.', dest='MLM_path', help='path to MLM')
    db_config_path = parser.parse_args().db_config_path

    # MOOCdb config
    if not db_config_path:
        moocdb = parser.parse_args().db
        MLM_path = parser.parse_args().MLM_path
        MLM_CONFIG_TEMPLATE['mysql']['host'] = 'localhost'
        MLM_CONFIG_TEMPLATE['mysql']['port'] = 3306
        MLM_CONFIG_TEMPLATE['mysql']['user'] = raw_input('Enter your username for MySQL: ')
        MLM_CONFIG_TEMPLATE['mysql']['password'] = getpass.getpass('Enter corresponding password: ')
    else:
        db_config_path = os.path.abspath(db_config_path)
        if not os.path.isfile(db_config_path):
            sys.exit('Specified MOOCdb config file does not exist.')
        db_cfg = yaml.safe_load(open(db_config_path))
        if not db_config_check(db_cfg, MOOCDB_CONFIG_SUB_STRUCTURE):
            sys.exit('MOOCdb config file is invalid.')
        # Exit with code 0 if not queued
        if not db_cfg['full_pipeline']['MLM']:
            print('MLM is not queued, container exited.')
            exit(0)
        moocdb = db_cfg['MOOCdb']['database']
        MLM_path = os.path.join(
            db_cfg['MOOCdb']['work_dir'],
            db_cfg['MOOCdb']['MLM_folder'] + '/'
        )
        MLM_CONFIG_TEMPLATE['mysql']['host'] = db_cfg['mysql']['host']
        MLM_CONFIG_TEMPLATE['mysql']['port'] = db_cfg['mysql']['port']
        MLM_CONFIG_TEMPLATE['mysql']['user'] = db_cfg['mysql']['user']
        MLM_CONFIG_TEMPLATE['mysql']['password'] = db_cfg['mysql']['password']

    # Check the MLM folder
    MLM_path = os.path.abspath(MLM_path)
    if not os.path.isdir(MLM_path):
        sys.exit('MLM directory does not exist.')
    MLM_files = [f for f in os.listdir(MLM_path)]
    if 'full_pipe.py' not in MLM_files:
        sys.exit("MLM directory is not complete.")
    MLM_exec = os.path.join(MLM_path, 'full_pipe.py')

    # Check moocdb database exist
    db = MySQLdb.connect(host=MLM_CONFIG_TEMPLATE['mysql']['host'],
                         port=MLM_CONFIG_TEMPLATE['mysql']['port'],
                         user=MLM_CONFIG_TEMPLATE['mysql']['user'],
                         passwd=MLM_CONFIG_TEMPLATE['mysql']['password'],
                         db=moocdb)
    cursor = db.cursor()
    cursor.execute("SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA " +
                   "WHERE SCHEMA_NAME = '%s'" % moocdb)
    if cursor.rowcount == 0:
        sys.exit('MOOCdb database dose not exist.')
    cursor.close()

    # Finally set the two paths after check
    MLM_CONFIG_TEMPLATE['mysql']['database'] = moocdb

    # Run MLM as subprocess
    print("Running MLM on database %s" % moocdb)
    config_dict = MLM_CONFIG_TEMPLATE
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yml') as config_file:
        yaml.dump(config_dict, config_file, default_flow_style=False)
        print(sys.executable + ' -u ' + MLM_exec + ' ' + config_file.name)
        p = subprocess.Popen(sys.executable + ' -u ' + MLM_exec + ' ' + config_file.name,
                             shell=True, stderr=subprocess.PIPE, bufsize=1)
        print("Process pid: %d" % p.pid)
        with p.stderr:
            for line in iter(p.stderr.readline, ''):
                print(line)
        p.wait()


if __name__ == "__main__":
    autorun()
