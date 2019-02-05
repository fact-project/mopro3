import click
from peewee import IntegrityError
from ..database import CorsikaSettings, initialize_database, database


@click.command()
@click.argument('name')
@click.argument('version', type=int)
@click.argument('config_h', type=click.Path(exists=True, dir_okay=False))
@click.argument('inputcard_template', type=click.Path(exists=True, dir_okay=False))
@click.option(
    '-a', '--additional-files',
    type=click.Path(exists=True, dir_okay=False),
    help='A tar.gz file with files to be unpacked into the corsika run directory'
)
def main(name, version, config_h, inputcard_template, additional_files):
    '''
    Insert a new CORSIKA config into the database

    Arguments:
    NAME: The identifier used for this version, e.g. epos_fluka_iact
    VERSION: Corsika version as integer, e.g. 76900
    CONFIG_H: The CORSIKA configuration header file, run coconut to create it in include/
    INPUTCARD_TEMPLATE: A jinja2 template for the corsika input card
    '''
    initialize_database()

    with open(config_h) as f:
        config_h = f.read()

    with open(inputcard_template) as f:
        inputcard_template = f.read()

    if additional_files is not None:
        with open(additional_files, 'rb') as f:
            additional_files = f.read()

    try:
        with database.atomic():
            CorsikaSettings.create(
                name=name,
                version=version,
                config_h=config_h,
                inputcard_template=inputcard_template,
                additional_files=additional_files,
            )
    except IntegrityError as e:
        print(f'Could not insert CORSIKA settings: {e}')
        print('The following combinations are already taken:')
        for s in CorsikaSettings.select(CorsikaSettings.name, CorsikaSettings.version):
            print(s.name, s.version)


if __name__ == '__main__':
    main()
