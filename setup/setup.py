import os
import yaml
import platform

import pathlib
import psycopg2


def load_nlp():
    """Create database and load the sample NLP data"""

    with open('./credentials', 'r') as credential_yaml:
        credentials = yaml.load(credential_yaml, Loader=yaml.Loader)

    with open('./config', 'r') as config_yaml:
        config = yaml.load(config_yaml, Loader=yaml.Loader)

    os.environ['PGPASSWORD'] = credentials['postgres']['password']

    # Create the database - if it exists an error will be thrown which can be ignored
    os.system("createdb -h {host} -U {user} -p {port} {database}".format(**credentials['postgres']))

    connection = psycopg2.connect(
        password=credentials['postgres']['password'],
        dbname=credentials['postgres']['database'],
        user=credentials['postgres']['user'],
        host=credentials['postgres']['host'],
        port=credentials['postgres']['port'])
    cursor = connection.cursor()

    # Here we use the config to insert named parameters into the query
    # The ** syntax unpacks a dictionary into keyword arguments to a function

    cursor.execute("""
    DROP TABLE IF EXISTS {app_name}_sentences_nlp352; CREATE TABLE {app_name}_sentences_nlp352 (docid text, sentid integer, wordidx integer[], words text[], poses text[], ners text[], lemmas text[], dep_paths text[], dep_parents integer[]);""".format(**config))

    cursor.execute("CREATE INDEX ON {app_name}_sentences_nlp352 (docid);".format(**config))

    cursor.execute("CREATE INDEX ON {app_name}_sentences_nlp352 (sentid);".format(**config))

    from_data = os.path.join(os.getcwd(), 'input', 'sentences_nlp352')
    if platform.system() == 'Windows':
        from_data = pathlib.PureWindowsPath(from_data)

    with open(str(from_data)) as data:
        cursor.copy_from(data, "{app_name}_sentences_nlp352".format(**config))
        connection.commit()



if __name__ == '__main__':
    load_nlp()
