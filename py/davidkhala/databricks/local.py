import pathlib

from davidkhala.utils.syntax.path import home_resolve

CONFIG_PATH = home_resolve('.databrickscfg')


def logout():
    pathlib.Path(CONFIG_PATH).unlink(True)
