from concurrent.futures import process
import datetime
from email.headerregistry import MessageIDHeader
import logging
import os, uuid
import azure.functions as func
from azure.storage.queue import (
        QueueClient,
        BinaryBase64EncodePolicy,
        BinaryBase64DecodePolicy
)
from azure.keyvault.secrets import SecretClient
from azure.identity import DefaultAzureCredential

STORAGE_ACCOUNT_SECRET_NAME = "asb-automation-storage-connection-string"

AKV_NAME = "baseline-staging-kv"
AKV_URI = f"https://{AKV_NAME}.vault.azure.net"
QUEUE_NAME = "staging-queue"

# Build AKV credential for retrieving secrets
def create_keyvault_credential():
	keyvault_credential = DefaultAzureCredential()
	akv_client = SecretClient(vault_url=AKV_URI, credential=keyvault_credential)
	return akv_client

# Retrieves a secret from Azure Key Vualt
def get_secret_from_akv(akv_client, secret_name):

	# Retrieve secrets from Azure Key Vault
	return akv_client.get_secret(secret_name).value

# Create the Azure Storage Queue Client that will be used to interact with the Queue
def create_azure_storage_queue_client(queue_name, conn_str):

    # Instantiate a QueueClient object which will be used to create and manipulate the queue
    queue_client = QueueClient.from_connection_string(conn_str, queue_name)

    return queue_client

# Process the message (Query Kusto -> Kickoff baseline markdown creation -> Pubish baseline markdown)
def process_message(queue_client):
    
    # Get the next message in the queue
    messages = queue_client.peek_messages()
    for message in messages:
    
        # Make call to Kusto and attempt to process the message
        # Placeholder for processing the next messgae
        logging.info(message.content)
    
    # If the message processes successfully, return true, otherwise return false
    return True

# Remove message from queue
def dequeue_message(queue_client):

    # Get the next message in the queue
    messages = queue_client.receive_messages()
    message = next(messages)

    # Deque the next message
    queue_client.delete_message(message.id, message.pop_receipt)
    return True

def main(mytimer: func.TimerRequest) -> None:
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()

    if mytimer.past_due:
        logging.info('The timer is past due!')

    logging.info('Python timer trigger function ran at %s', utc_timestamp)
    # Create Azure Key Vault Client

    akv_client = create_keyvault_credential()

    # Retrieve Storage Account Connection String
    storage_account_connection_string = get_secret_from_akv(akv_client, STORAGE_ACCOUNT_SECRET_NAME)

    # Create a Queue Client to be used for interacting with the Storage Account Queue
    queue_client = create_azure_storage_queue_client(QUEUE_NAME, storage_account_connection_string)

    # Peek at the first message
    j = queue_client.peek_messages()

    message_processed_successfully = process_message(queue_client)

    if message_processed_successfully:
        # If the message processed successfully, dequeue the message:
        dequeue_message(queue_client)
