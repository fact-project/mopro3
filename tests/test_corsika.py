from mopro.config import config

config.load_yaml('tests/test_config.yaml')


def test_create_run():
    from mopro.database import (
        database,
        initialize_database,
        setup_database,
        CorsikaRun,
        CorsikaSettings,
    )

    initialize_database()
    setup_database()

    with database.transaction():
        c = CorsikaSettings()
        c.name = 'epos_fluka_iact'
        c.config_h = open('examples/epos_fluka_iact.h').read()
        c.inputcard_template = open('examples/inputcard_template.txt').read()
        c.version
        c.save()

    with database.transaction():
        r = CorsikaRun()
        r.corsika_settings = c
        r.energy_min = 100
        r.energy_max = 200e3
        r.max_radius = 300
        r.zenith_min = 0
        r.zenith_max = 5
        r.azimuth_min = 0
        r.azimuth_max = 10
        r.primary_particle = 1
        r.spectral_index = -2.7
        r.viewcone = 0
        r.reuse = 1
        r.save()

    c.format_input_card(r, 'test.eventio')

    assert CorsikaRun.select().count() == 1
