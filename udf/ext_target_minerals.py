# ==============================================================================
# TARGET ADJECTIVE EXTRACTOR
# ==============================================================================

# import relevant modules and data
# ==============================================================================
import time
import random
import re
import yaml
import psycopg2
from psycopg2.extensions import AsIs
from yaml import Loader

from download_csv import download_csv

start_time = time.time()

# Connect to Postgres
with open('./credentials', 'r') as credential_yaml:
    credentials = yaml.load(credential_yaml, Loader=Loader)

with open('./config', 'r') as config_yaml:
    config = yaml.load(config_yaml, Loader=Loader)

# Connect to Postgres
connection = psycopg2.connect(
    password=credentials['postgres']['password'],
    dbname=credentials['postgres']['database'],
    user=credentials['postgres']['user'],
    host=credentials['postgres']['host'],
    port=credentials['postgres']['port'])
cursor = connection.cursor()

# IMPORT TARGETS WITH DEPENDENTS
cursor.execute("""
    SELECT docid, sentid, target_id, target_word, target_children
    FROM target_instances
    WHERE target_children<>'[[]]';
""")

target = cursor.fetchall()

# IMPORT THE SENTENCES DUMP
cursor.execute("""
    WITH temp as (
            SELECT DISTINCT ON (docid, sentid) docid, sentid
		     FROM target_instances
            WHERE target_children<>'[[]]'
    )


    SELECT s.docid, s.sentid, words, poses
    FROM %(my_app)s_sentences_%(my_product)s AS s

	JOIN temp ON temp.docid=s.docid AND temp.sentid=s.sentid;
    """, {
    "my_app": AsIs(config['app_name']),
    "my_product": AsIs(config['product'].lower())
})

sentences = cursor.fetchall()

# initalize the target_instances table
cursor.execute("""
    DELETE FROM target_minerals;
""")

# push drop/create to the database
connection.commit()

# Grab the mineral metadata from Macrostrat
minerals = download_csv('https://macrostrat.org/api/v2/defs/minerals?all&format=csv')
mineral_names = [name.lower() for name in minerals['mineral']]

adj = []
for idx, line in enumerate(target):
    docid, sentid, target_id, target_word, target_children = line
    target_children = eval(target_children)
    target_children = target_children[0]


    sent = [elem for elem in sentences if elem[0]
            == docid and elem[1] == sentid]

    for c in target_children:
        pos = sent[0][3][c]
        token = sent[0][2][c]
        print(pos, token)
        if pos == 'NN' and token.lower() in mineral_names:

            # TODO add mineral link and ID
            # write to PSQL table
            cursor.execute("""
                INSERT INTO target_minerals(   docid,
                                                sentid,
                                                target_id,
                                                target_word,
                                                target_mineral)

                VALUES (%s, %s, %s, %s, %s);""",
                           (docid, sentid, target_id,
                            target_word, token)
                           )
        if c < 0:
            print('something is up!')

# push insertions to the database
connection.commit()

# close the connection
connection.close()
