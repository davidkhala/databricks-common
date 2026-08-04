import configparser
import os

p = os.path.expanduser('~/.databrickscfg')
c = configparser.ConfigParser()
c.read(p)
c['DEFAULT']['serverless_compute_id'] = 'auto'
with open(p, 'w') as f:
    c.write(f)
