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
        result = urllib2.urlopen(request)
        
        if result.code == 200:
            
            output = cStringIO.StringIO()
            output.write(result.read())
            hex_string='c4a89d66879af372f05d160fefaabbdd'
            encoded = hex_string.decode("hex")
            print(encoded.encode("base64"))
            bucketName = os.environ['bucketName']
            s3_config = Config(signature_version='s3v4')
            s3 = boto3.client('s3', config=s3_config)
            SSEKMSKeyId=os.environ['SSEKMSKeyId']
            s3.put_object(
                Key=fileName,
                Body=output.getvalue(),
                Bucket=bucketName,
                ContentMD5=encoded.encode("base64"),
                SSEKMSKeyId=SSEKMSKeyId,
                ServerSideEncryption='aws:kms',
                ACL='private',
                Metadata={
                    'md5chksum': 'lTCOfxkMe+PoPRgwdHEIWg=='
                },
                )
            
            
            print('File added to S3 bucket successfully!!')
        else:
            print('Error!!')
    except urllib2.HTTPError, e:

        print('HTTPError!!')
        checksLogger.error('HTTPError = ' + str(e.code))
    except urllib2.URLError, e:

        print('URLError!!')
        checksLogger.error('URLError = ' + str(e.reason))
    except HashMD5Exception:

        print('ERROR = MD5 Exception occured !!')
    except Exception, e:

        print('GenericError!!')
        checksLogger.error('generic exception = ' + str(e))