#!/usr/bin/python
# -*- coding: utf-8 -*-

from __future__ import print_function
import boto3
import urllib2
import base64
import cStringIO
import logging
import os
from botocore.client import Config
from cStringIO import StringIO
import datetime
import time
import hashlib


def lambda_handler(event, context):
    worldCheckFileLoad()


class HashMD5Exception(Exception):

    pass

# downloads the Worldcheckfile and saves it into S3 bucket.


def worldCheckFileLoad():

    url = os.environ['url']
    username = os.environ['username']
    password = os.environ['password']
    fileName = os.environ['fileName']
    request = urllib2.Request(url, headers={'accept': '*/*'})
    base64string = base64.b64encode('%s:%s' % (username, password))
    request.add_header('Authorization', 'Basic %s' % base64string)
    urlHash = hashlib.md5()
    dloadHash = hashlib.md5()
    try:
                # calling the md5 function to get the MD5 checksum of the file
                # which will help us maintain the integrity of the file that we
                # download and place into S3 bucket
        md5 = getMD5Chcksum(username, password)
        hex_string, md5FileName = md5.split("  ")
        # reads the data of Worldcheck file from the URL.
        result = urllib2.urlopen(request)

        if result.code == 200:

            output = cStringIO.StringIO()
            output.write(result.read())
            decoded = hex_string.strip().decode('hex')
            bucketName = os.environ['bucketName']
            s3_config = Config(signature_version='s3v4')
            s3 = boto3.client('s3', config=s3_config)
            SSEKMSKeyId = os.environ['SSEKMSKeyId']
            encodedBase64 = base64.b64encode(decoded)
            # put_object() helps to put the file into S3 bucket.
            s3.put_object(
                Key=fileName,
                Body=output.getvalue(),
                Bucket=bucketName,
                ContentMD5=encodedBase64,
                SSEKMSKeyId=SSEKMSKeyId,
                ServerSideEncryption='aws:kms',
                ACL='private',
                Metadata={'md5': hex_string, 'base64': encodedBase64},
            )

            print('File added to S3 bucket successfully!!')
        else:
            print('Error!!')
    except urllib2.HTTPError as e:

        print('HTTPError!!')
        checksLogger.error('HTTPError = ' + str(e.code))
    except urllib2.URLError as e:

        print('URLError!!')
        checksLogger.error('URLError = ' + str(e.reason))
    except HashMD5Exception:

        print('ERROR = MD5 Exception occured !!')
    except Exception as e:

        print('GenericError!!')
        checksLogger.error('generic exception = ' + str(e))

# The Function needs username and password required to access url and
# returns the data that is read from the file.


def getMD5Chcksum(username, password):
    try:
        md5Url = os.environ['md5Url']
        request = urllib2.Request(md5Url, headers={'accept': '*/*'})
        base64string = base64.b64encode('%s:%s' % (username, password))
        request.add_header('Authorization', 'Basic %s' % base64string)
        # Reads the data from MD5 file which will be used as checksum for
        # worldcheck file.
        result = urllib2.urlopen(request)
    except urllib2.HTTPError as e:
        print('HTTPError!!')
        checksLogger.error('HTTPError = ' + str(e.code))
    except urllib2.URLError as e:
        print('URLError!!')
        checksLogger.error('URLError = ' + str(e.reason))
    except Exception as e:
        print('GenericError!!')
        checksLogger.error('generic exception = ' + str(e))
    return result.read()
