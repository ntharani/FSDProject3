#!/usr/bin/env python3
#
# Udacity FSD Nanodegree Project #3
# Log Analysis

from __future__ import print_function

import psycopg2
from tabulate import tabulate

DBNAME = "news"


def get_query1():
    """DB Query - returns top 3 most popular three articles of all time."""
    db = psycopg2.connect(database=DBNAME)
    c = db.cursor()
    c.execute("""
    SELECT articles.title, count(path) from log
        JOIN articles ON (substring(path from 10) = articles.slug)
        GROUP BY path, title
        ORDER BY count DESC
        LIMIT 3
    """)
    q1 = c.fetchall()
    db.close()
    return q1


def get_query2():
    """DB Query - returns most popular article authors of all time,
    accounting for their combined pageviews"""
    db = psycopg2.connect(database=DBNAME)
    c = db.cursor()
    c.execute("""
    SELECT authors.name, count(log.path) AS views
        FROM authors
        JOIN articles
        ON authors.id = articles.author
        JOIN log
        ON substring(path from 10) = articles.slug
        GROUP BY authors.name
        ORDER BY views DESC;
    """)
    q2 = c.fetchall()
    db.close()
    return q2


def get_query3():
    """DB Query - Which days had > 1% 404 Errors"""
    db = psycopg2.connect(database=DBNAME)
    c = db.cursor()
    c.execute("""
    WITH
        bad_reqs AS (
        SELECT to_char(time,'YYYY-MM-DD') AS cleaned_date, COUNT(*) AS count
        FROM log WHERE log.status = '404 NOT FOUND'
        GROUP BY cleaned_date
        ),
        total_reqs AS (
        SELECT to_char(time,'YYYY-MM-DD') AS cleaned_date, COUNT(*) as count
        FROM log
        GROUP BY cleaned_date
        )
        SELECT bad_reqs.cleaned_date, bad_reqs.count*1.0 / total_reqs.count
        FROM bad_reqs, total_reqs
        WHERE bad_reqs.cleaned_date = total_reqs.cleaned_date
        AND bad_reqs.count*1.0 / total_reqs.count > 0.01;
    """)
    q3 = c.fetchall()
    db.close()
    return q3


def showanswer():
    """Prints the results of q1, q2 and q3"""
    q1results = get_query1()
    q2results = get_query2()
    q3results = get_query3()

    # print(q1results)
    # for k,v in q1results:
    #     print(k, v)

    ans1 = tabulate(
        q1results,
        headers=["Article Title", "Views"],
        tablefmt="psql")
    ans2 = tabulate(
        q2results,
        headers=["Author", "Views Across All Articles"],
        tablefmt="psql")
    ans3 = tabulate(
        q3results,
        headers=["Date", "Error Rate"],
        tablefmt="psql")

    print("\n" + "Q1: Top 3 most popular articles of all time.")
    print(ans1)
    print("\n" + "Q2: Most popluar authors across all their articles")
    print(ans2)
    print("\n" + "Q3: Days where more than 1% of requests led to errors")
    print(ans3)

showanswer()
