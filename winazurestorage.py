#!/usr/bin/env python
# encoding: utf-8
"""
Python wrapper around Windows Azure storage
Sriram Krishnan <sriramk@microsoft.com>
"""

import base64
import hmac
import httplib
import hashlib
import time
import urllib
import urlparse
import sys
import os
from xml.dom import minidom #TODO: Use a faster way of processing XML

DEVSTORE_ACCOUNT = "devstoreaccount1"
DEVSTORE_SECRET_KEY = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="


DEVSTORE_HOST="127.0.0.1:10000"
CLOUD_HOST = "blob.core.windows.net"


PREFIX_PROPERTIES = "x-ms-prop-"
PREFIX_METADATA = "x-ms-meta-"
PREFIX_STORAGE_HEADER = "x-ms-"

NEW_LINE = "\x0A"

DEBUG = False

TIME_FORMAT ="%a, %d %b %Y %H:%M:%S %Z"


class WAStorageConnection:
    def __init__(self, server_url = DEVSTORE_HOST, account_name=DEVSTORE_ACCOUNT,secret_key = DEVSTORE_SECRET_KEY):
        self.server_url = server_url
        self.account_name = account_name
        self.secret_key = base64.decodestring(secret_key)
        
       
    def create_container(self,container_name, is_public):
        headers ={}
        if is_public:
            headers[PREFIX_PROPERTIES+ "publicaccess"] = "true"
            
        return self._do_store_request(container_name, None, 'PUT', headers)
  
    def put_blob(self, container_name, key, data, content_type=None):
        return self._do_store_request(container_name, key, 'PUT', {}, data, content_type )
           
    def get_blob(self, container_name, key):
        response = self._do_store_request(container_name, key, 'GET' )
        return response.read()
                
    def list_containers(self):
        #TODO: Deal with nextmarker for large requests (only 5K containers returned by default)
        #TODO: Use a different XML parsing scheme
        
        response = self._do_store_request(query_string = "?comp=list")
        
        dom = minidom.parseString(response.read())
        
        containers = dom.getElementsByTagName("Container")
        for container in containers:
            container_name = container.getElementsByTagName("Name")[0].firstChild.data
            etag = container.getElementsByTagName("Etag")[0].firstChild.data
            last_modified = time.strptime(container.getElementsByTagName("LastModified")[0].firstChild.data, TIME_FORMAT)
            yield (container_name, etag, last_modified)
        
        dom.unlink() #Docs say to do this to force GC. Ugh.
  
    def _get_auth_header(self, http_method, path, data, headers):
        string_to_sign =""
        
        #First element is the method
        string_to_sign += http_method + NEW_LINE
        
        #Second is the optional content MD5
        string_to_sign += NEW_LINE
        
        #content type - this should have been initialized atleast to a blank value
        if headers.has_key("content-type"):
            string_to_sign += headers["content-type"] 
        string_to_sign += NEW_LINE
        
        # date - we don't need to add header here since the special date storage header
        # always exists in our implementation
        string_to_sign +=  NEW_LINE
        
        # Construct canonicalized headers. 
        # TODO: Note that this doesn't implement parts of the spec - combining header fields with same name,
        # unfolding long lines and trimming white spaces around the colon
        
        ms_headers =[header_key for header_key in headers.keys() if header_key.startswith(PREFIX_STORAGE_HEADER)]
        ms_headers.sort()
        for header_key in ms_headers:
            string_to_sign += "%s:%s%s" % (header_key, headers[header_key], NEW_LINE) 
        
        # Add canonicalized resource
        string_to_sign += "/" + self.account_name + path
        utf8_string_to_sign = unicode(string_to_sign).encode("utf-8")
        hmac_digest = hmac.new(self.secret_key, utf8_string_to_sign, hashlib.sha256).digest()
        return base64.encodestring(hmac_digest).strip()
        
        
        
    def _do_store_request(self, container=None, blob_name=None, http_method="GET", headers = {},data = "", 
                    content_type=None, query_string = None, signed=True):
        connection = httplib.HTTPConnection(self.server_url)
        
        # Construct right path based on account name , container name and blob name if any
        path = "/" + self.account_name + "/"
        if container!= None:
            path = path  + container +"/"
            if blob_name != None:
                path = path + blob_name
        
        if query_string != None:
            path = path + query_string
      
        
        headers[PREFIX_STORAGE_HEADER + "date"] = time.strftime("%a, %d %b %Y %H:%M:%S GMT", time.gmtime()) #RFC 1123
        if content_type != None:
            headers["content-type"] = content_type  
               
        if signed:
            auth_header = self._get_auth_header(http_method,path, data, headers)
            headers["Authorization"] = "SharedKey " + self.account_name + ":" + auth_header
            headers["content-length"] = len(data)
         
        connection.request(http_method, path, data, headers)
        response = connection.getresponse()
        if DEBUG:
            print response.status
        return response

    
def main():
    conn = WAStorageConnection()
    for (container_name,etag, last_modified ) in  conn.list_containers():
        print container_name
        print etag
        print last_modified
        
    conn.create_container("testcontainer", False)
    conn.put_blob("testcontainer","test","Hello World!" )
    print conn.get_blob("testcontainer", "test")


if __name__ == '__main__':
    main()      
      
