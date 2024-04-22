from dataclasses import dataclass
from dotenv import load_dotenv
from byepy import connect, DEFINE, register_composite, to_compile, SQL
import psycopg2
import time

import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Connect to the database using environment variables
conn = psycopg2.connect(
    dbname=os.getenv("DB_NAME"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD"),
    host=os.getenv("DB_HOST"),
    port=os.getenv("DB_PORT")
)

DEFINE(
    """
    CREATE TABLE IF NOT EXISTS tweets_small
        ( sentiment int
        , id  varchar
        , date varchar
        , flag varchar
        , user_handle varchar
        , text varchar
        );
    """
)

# Define your data class
@register_composite
@dataclass
class tweets_small:
    sentiment: int
    id: str
    date: str
    flag: str
    user_handle: str
    text: str


def build_tweet_dict_unoptimized():
    sentiments = {0: {}, 4: {}}

    # Connect to the database
    conn = psycopg2.connect(
        dbname="postgres",
        user="School",
        password="4215265",
        host="localhost",
        port="5432"
    )
    cur = conn.cursor()

    # Iterate through each sentiment level
    for sentiment in [0, 4]:
        # Execute the query to fetch tweets with the current sentiment
        cur.execute("SELECT text FROM tweets_small WHERE sentiment = %s", (sentiment,))
        tweets_small = cur.fetchall()

        # Iterate through each tweet for the current sentiment
        for twt in tweets_small:
            text = twt[0]  # Assuming the text is in the first column
            # Split the tweet into words
            words = text.split(" ")

            # Count instances of each word and update the dictionary
            for word in words:
                # Initialize the word entry if it doesn't exist
                if word not in sentiments[sentiment]:
                    sentiments[sentiment][word] = {'instances': 0, 'total': 0}

                # Increment instances count
                sentiments[sentiment][word]['instances'] += 1

                # Increment total count in the database
                cur.execute("SELECT COUNT(*) FROM tweets_small WHERE text LIKE %s", (f"%{word}%",))
                count = cur.fetchone()[0]
                sentiments[sentiment][word]['total'] = count

    # Close cursor and connection
    cur.close()
    conn.close()

    return sentiments


@to_compile
def fetch_tweets_and_count():
    sentiments = {0: {}, 4: {}}

    # Iterate through each sentiment level
    for sentiment in [0, 4]:
        # Execute the query to fetch tweets with the current sentiment
        tweets_small = SQL(
            f"""
            SELECT ARRAY_AGG(text)
            FROM (
                SELECT text
                FROM tweets_small
                WHERE sentiment = {sentiment}
            ) AS _;
            """
        )

        # Iterate through each tweet for the current sentiment
        for text in tweets_small:
            # Split the tweet into words
            words = text.split(" ")

            # Count instances of each word and update the dictionary
            for word in words:
                # Initialize the word entry if it doesn't exist
                if word not in sentiments[sentiment]:
                    sentiments[sentiment][word] = {'instances': 0, 'total': 0}

                # Increment instances count
                sentiments[sentiment][word]['instances'] += 1

                escaped_word = word.replace("'", "''")

                # Increment total count in the database
                total_count = SQL(
                    f"""
                    SELECT COUNT(*) FROM tweets_small WHERE text LIKE '%%{escaped_word}%%';
                    """
                )
                sentiments[sentiment][word]['total'] = total_count

    return sentiments


def time_function(func):
    start_time = time.time()
    result = func()
    end_time = time.time()
    print(f"{func.__name__} executed in {end_time - start_time} seconds")
    return result


if __name__ == "__main__":
    build_tweet_dict_unoptimized_timed = time_function(build_tweet_dict_unoptimized)
    fetch_tweets_and_count_timed = time_function(fetch_tweets_and_count)
