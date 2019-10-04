""" Extract mentions of minerals and link to mindat"""
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

# Import target words
cursor.execute("""
    SELECT docid, sentid, target_id, target_word, target_children
    FROM target_instances
""")

target = cursor.fetchall()

# Import the sentences linked to target words
cursor.execute("""
    WITH temp as (
            SELECT DISTINCT ON (docid, sentid) docid, sentid
		     FROM target_instances
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

    # find the corresponding sentence (docid & sentid)
    # TODO consider looking for a set of related sentences either side
    sentence_list = [elem for elem in sentences if elem[0]
            == docid and elem[1] == sentid]

    for sent in sentence_list:
        print(sent)
        tokens = sent[2]
        parts_of_speech = sent[3]

        for index, pos in enumerate(parts_of_speech):
            token = tokens[index]
            print(pos, token)
            if pos == 'NN' and token.lower() in mineral_names:

                mineral_index = mineral_names.index(token.lower())
                mineral_link = minerals['url'][mineral_index]
                mineral_id = minerals['mineral_id'][mineral_index]

                # write to PSQL table
                cursor.execute("""
                    INSERT INTO target_minerals(   docid,
                                                    sentid,
                                                    target_id,
                                                    target_word,
                                                    target_mineral,
                                                    target_mineral_link,
                                                    target_mineral_id)

                    VALUES (%s, %s, %s, %s, %s, %s, %s);""",
                               (docid, sentid, target_id,
                            target_word, token, mineral_link, mineral_id)
                           )

# push insertions to the database
connection.commit()

# close the connection
connection.close()
