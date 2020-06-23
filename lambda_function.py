from urllib.parse import unquote
from json import dumps
import gzip
import logging
import datetime
from io import BytesIO
import os

import boto3
from botocore.exceptions import ClientError
import requests
import lxml
from bs4 import BeautifulSoup

# Config file
logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s')
RSS_URL = os.getenv('RSS_URL')
AWS_ACCESS_KEY = os.getenv('AWS_ACCESS_KEY')
AWS_SECRET_KEY = os.getenv('AWS_SECRET_KEY')
AWS_REGION = os.getenv('AWS_REGION')
AWS_S3_BUCKET = os.getenv('AWS_S3_BUCKET')


def get_rss(rss_url):
    """Get RSS XML and parse it to BeautifulSoup object

    :param rss_url: a string, URL of RSS from upwork
    :return: BeautifulSoup xml object if successful, or boolean False if failed
    """
    url = rss_url

    querystring = {}
    payload = ""
    headers = {}

    response = requests.request("GET", url, data=payload, headers=headers, params=querystring)
    if response.status_code == 200:
        return BeautifulSoup(response.content, "lxml")
    else:
        return False


def get_skills(rss):
    """ Get list of skills from Upwork's RSS page

    :param rss: BeautifulSoup xml object
    :return: a list, containing skills retrieved, can be an empty list
    """
    result = []
    for item in rss.find_all("item"):
        content_encoded = item.encoded.get_text()

        try:
            skills = content_encoded.split("Skills</b>:", 1)[1]
        except IndexError as e:
            logging.error("%s: %s" % (e, content_encoded))
            continue

        try:
            skills = skills.split("<br />", 1)[0]
        except AttributeError as e:
            logging.error("%s: %s" % (e, skills))
            continue
        except TypeError as e:
            logging.error("%s: %s" % (e, skills))
            continue

        try:
            skills = skills.split(",")
        except TypeError as e:
            logging.error("%s: %s" % (e, skills))
            continue
        except AttributeError as e:
            logging.error("%s: %s" % (e, skills))
            continue

        for skill in skills:
            skill = unquote(skill)
            result.append(skill.strip())

    return result


def upload_fileobj(fileobj_name, bucket, object_name=None):
    """Upload a file to an S3 bucket

    :param fileobj_name: File object to upload
    :param bucket: Bucket to upload to
    :param object_name: S3 object name. If not specified then file_name is used
    :return: True if file was uploaded, else False
    """

    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = fileobj_name

    # Upload the file
    s3_client = boto3.client('s3', aws_access_key_id=AWS_ACCESS_KEY, aws_secret_access_key=AWS_SECRET_KEY)
    try:
        response = s3_client.upload_fileobj(fileobj_name, bucket, object_name)
    except ClientError as e:
        logging.error(e)
        return False
    return True


def lambda_handler(event, context):
    jobs = get_rss(RSS_URL)
    if jobs:
        title = jobs.rss.channel.title.get_text()
        link = jobs.rss.channel.link.get_text()
        description = jobs.rss.channel.description.get_text()
        pubdate = jobs.rss.channel.pubDate.get_text()
        retrieved_skill = get_skills(jobs)
        content = {
            "title": title,
            "link": link,
            "description": description,
            "pubdate": pubdate,
            "skills": retrieved_skill,
        }
        serialized_content = dumps(content)

        # Create compress byte-like object
        compressed_content = gzip.compress(serialized_content.encode("utf-8"))
        f = BytesIO(compressed_content)

        # Upload to S3
        current_time = str(datetime.datetime.now(datetime.timezone.utc)).replace(" ", "_")
        object_name = "upwork_skills_" + current_time + ".json.gz"
        upload_fileobj(f, AWS_S3_BUCKET, object_name)
        logging.info("Skills upload successful")
        return True
    else:
        logging.error("Problem occurred")
        return False


if __name__ == '__main__':
    lambda_handler()
