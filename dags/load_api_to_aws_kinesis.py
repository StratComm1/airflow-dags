# Project 9 deployment check

import json
import logging
from datetime import datetime, timezone

import boto3
import requests
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator


logger = logging.getLogger(__name__)

API_POSTS_URL = "https://jsonplaceholder.typicode.com/posts"
AWS_REGION = "us-east-1"
KINESIS_STREAM_NAME = "user-posts-data-stream"
USER_ID_VARIABLE = "api_user_id"


def _set_api_user_id():
    """Select the next user ID, cycling from 1 through 10."""
    previous_id = int(Variable.get(USER_ID_VARIABLE, default_var="-1"))
    user_id = previous_id + 1 if 1 <= previous_id < 10 else 1

    Variable.set(USER_ID_VARIABLE, user_id)
    logger.info("Selected API user ID %s", user_id)
    return user_id


def _extract_userposts(new_api_user_id):
    """Fetch the selected user's posts and pass them to the write task."""
    user_id = int(new_api_user_id)

    response = requests.get(
        API_POSTS_URL,
        params={"userId": user_id},
        timeout=30,
    )
    response.raise_for_status()
    posts = response.json()

    if not isinstance(posts, list) or not posts:
        raise ValueError(f"Expected a nonempty list of posts for user {user_id}")

    for post in posts:
        if not isinstance(post, dict) or post.get("userId") != user_id:
            raise ValueError(f"Received an unexpected post for user {user_id}")

    logger.info("Fetched %s posts for user %s", len(posts), user_id)
    return posts


def _process_user_posts(user_posts):
    """Publish one newline-delimited JSON record per post to Kinesis."""
    kinesis_client = boto3.client("kinesis", region_name=AWS_REGION)

    for post in user_posts:
        record = (json.dumps(post) + "\n").encode("utf-8")
        kinesis_client.put_record(
            StreamName=KINESIS_STREAM_NAME,
            Data=record,
            PartitionKey=str(post["userId"]),
        )

    logger.info(
        "Published %s posts to Kinesis stream %s in %s",
        len(user_posts),
        KINESIS_STREAM_NAME,
        AWS_REGION,
    )
    return len(user_posts)


with DAG(
    dag_id="load_api_aws_kinesis",
    default_args={"owner": "Kalea"},
    start_date=datetime(2023, 9, 24, tzinfo=timezone.utc),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["project-9", "api", "kinesis"],
) as dag:

    get_api_userId_params = PythonOperator(
        task_id="get_api_userId_params",
        python_callable=_set_api_user_id,
    )

    extract_userposts = PythonOperator(
        task_id="extract_userposts",
        python_callable=_extract_userposts,
        op_kwargs={"new_api_user_id": get_api_userId_params.output},
    )

    write_userposts_to_stream = PythonOperator(
        task_id="write_userposts_to_stream",
        python_callable=_process_user_posts,
        op_kwargs={"user_posts": extract_userposts.output},
    )
