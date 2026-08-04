import configparser
import pathlib

from davidkhala.utils.syntax.path import home_resolve

CONFIG_PATH = home_resolve('.databrickscfg')


def logout():
    pathlib.Path(CONFIG_PATH).unlink(True)


class Profile:
    def __init__(self, profile: str = 'DEFAULT'):
        self.profile = profile
        self.config = configparser.ConfigParser()
        self.config.read(CONFIG_PATH)

    def set(self, key: str, value: str):
        if self.profile not in self.config:
            self.config[self.profile] = {}
        self.config[self.profile][key] = value
        with open(CONFIG_PATH, 'w') as f:
            self.config.write(f)

    def get(self, key: str) -> str | None:
        return self.config.get(self.profile, key, fallback=None)

    def enable_serverless(self):
        self.set('serverless_compute_id', 'auto')
