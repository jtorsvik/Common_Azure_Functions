#!/usr/bin/env python
# coding: utf-8

# In[1]:


import os, uuid, sys
from azure.storage.filedatalake import DataLakeServiceClient
from azure.core._match_conditions import MatchConditions
from azure.storage.filedatalake._models import ContentSettings

from azure.keyvault.secrets import SecretClient
from azure.identity import DefaultAzureCredential


# In[2]:


def get_keyVault_secret( vault_url, secret_name ):
    try:
        secret_client = SecretClient(vault_url=vault_url, credential=credential)
        secret = secret_client.get_secret(secret_name)
        
        return secret
    
    except Exception as e:
        print(e)


# In[3]:


def get_credential(managed_identity_client_id):
    try:
        global credential
        credential = DefaultAzureCredential(managed_identity_client_id=managed_identity_client_id)
        return True
    except Exception as e:
        print(e)
        return False


# In[4]:


def initialize_storage_account_ad(storage_account_name):
    
    try:  
        global service_client
        service_client = DataLakeServiceClient(account_url="{}://{}.dfs.core.windows.net".format(
            "https", storage_account_name), credential=credential)
    
    except Exception as e:
        print(e)


# In[5]:


def create_directory(adls_container, adls_path):
    try:
        adls_container.create_directory(adls_path)
    
    except Exception as e:
        print(e)


# In[6]:


def upload_file_to_directory_bulk(adls_container, adls_path, adls_filename, local_path):
    try:

        file_system_client = service_client.get_file_system_client(file_system=adls_container)

        directory_client = file_system_client.get_directory_client(adls_path)
        
        file_client = directory_client.get_file_client(adls_filename)

        local_file = open(local_path,'r')

        file_contents = local_file.read()

        file_client.upload_data(file_contents, overwrite=True)

    except Exception as e:
        print(e)


# In[7]:


def list_directory_contents(adls_container, adls_path):
    try:
        
        file_system_client = service_client.get_file_system_client(file_system=adls_container)

        paths = file_system_client.get_paths(path=adls_path)

        for path in paths:
            print(path.name + '\n')

    except Exception as e:
        print(e)


# In[8]:


def read_file_from_adls( adls_path, file_name, adls_container):
    try:
        file_system_client = service_client.get_file_system_client(file_system=adls_container)

        directory_client = file_system_client.get_directory_client( adls_path)

        file_client = directory_client.get_file_client(file_name)

        download = file_client.download_file()

        downloaded_bytes = download.readall()
        
        return downloaded_bytes

    except Exception as e:
        print(e)

