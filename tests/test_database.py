from mopro.config import config


config.load_yaml('tests/test_config.yaml')


def test_init_database():
    from mopro.database import initialize_database

    initialize_database()


def test_setup_database():
    from mopro.database import initialize_database, setup_database

    initialize_database()
    setup_database()


def test_setup_twice_database():
    from mopro.database import initialize_database, setup_database

    initialize_database()
    setup_database()
    setup_database()
