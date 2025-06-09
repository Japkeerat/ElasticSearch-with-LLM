# Fake Data Generator

This module generates synthetic data for audit logs on a platform and puts the entire data in elasticsearch.

To use so, make a copy of `.env.template` as `.env` file and populate the fields required for ElasticSearch. 
(For demo, Japkeerat has shared his credentials which will automatically expire in 2 days. You can use it to test the code.)

To execute, run `python main.py` and it will generate 10,000 records in ElasticSearch.