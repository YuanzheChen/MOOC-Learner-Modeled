#!/usr/bin/env python
'''
This is the parser of YAML configuration file.

Parse the YAML config file and store all configuration variables as constants.
'''
import yaml
import getpass


class ConfigParser(object):
    '''
    Handles parsing and processing the config YAML file and
    act as an interface for other modules to read config information.
    '''

    CONFIG_STRUCTURE = {
        "mysql": {
            "query_user": None,
            "database": None,
            "query_password": None,
            "host": None,
            "user": None,
            "query_database": None,
            "password": None,
            "port": None
        }
    }

    def __init__(self, path='./config/config.yml'):
        # Parse YAML file and check validity
        self.cfg = yaml.safe_load(open(path))
        self.validity = self.check()
        if self.validity:
            self.pre_process()

    def check(self):
        # Check whether the structure of cfg is valid
        return self.dict_structure(self.cfg) == self.CONFIG_STRUCTURE

    def dict_structure(self, d):
        # Extract out the structure of dict cfg to
        # compare with the legal structure
        if isinstance(d, dict):
            return {k: self.dict_structure(d[k]) for k in d}
        else:
            # Replace all non-dict values with None.
            return None

    def is_valid(self):
        return self.validity

    def pre_process(self):
        pass

    def get_or_query_mysql(self):
        cfg_mysql = self.cfg['mysql']
        if cfg_mysql['query_user']:
            cfg_mysql['user'] = raw_input('Enter your username for MySQL: ')
        if cfg_mysql['query_password']:
            cfg_mysql['password'] = getpass.getpass('Enter corresponding password of user %s: ' % cfg_mysql['user'])
        if cfg_mysql['query_database']:
            cfg_mysql['database'] = raw_input('Enter the database get_redis_key: ')
        credential_list = [
            'host',
            'port',
            'user',
            'password',
            'database'
        ]
        return {k: cfg_mysql[k] for k in credential_list if k in cfg_mysql}
