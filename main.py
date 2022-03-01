from importlib import import_module
from dynaconf import Dynaconf


def main():
    settings = Dynaconf(envvar_prefix='PYRAMI').as_dict()
    app = import_module(settings['APP'])
    app.run({k: v for k, v in settings.items() if k != 'APP'})

if __name__ == '__main__':
    main()