import datetime
import logging
from .functions.authentication import getAccessToken
from .functions.funcs import REFRESHDATASET
from .functions.funcs import sendTeamsAlert
import azure.functions as func
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient


def main(mytimer: func.TimerRequest) -> None:
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()

    if mytimer.past_due:
        logging.info('The timer is past due!')

    # Set parameters for API
    domain = 'hu.nl'
    resource = 'https://analysis.windows.net/powerbi/api'

    try:
        import os
        # Load API connection secrets from Vault connection in function app
        clientId = os.getenv('clientId')
        clientSecret = os.getenv('clientSecret')
        print(
            f'Client ID and client secret succesfully loaded from Azure keyvault!')
    except Exception as e:
        print(
            f'Client ID and client secret could not be loaded from Azure keyvault!')
        print(e)

    # workspace name can not contain special characters! 
    workspaceName = 'prod datasources'

    # get acces token
    try:
        accessToken = getAccessToken(
            resource, clientId, clientSecret, domain)
        print(
            f'AccessToken succesfully loaded from microsoftonline!')
    except Exception as e:
        print(
            f'AccessToken could not be loaded from microsoftonline!')
        print(e)

    # Start application
    apiconn = REFRESHDATASET(accessToken, workspaceName)

    # Get refresh info
    results = apiconn.getRefreshInfo()
    failed = results[results['status'] == 'Failed']
    failed.reset_index(drop=True, inplace=True)

    try:
        import os
        # Load webhook secrets from Vault connection in function app
        webhook = os.getenv('webhook')
        incomingwebhook = os.getenv('incomingwebhook')
        print(
            f'Webhook secrets succesfully loaded from Azure keyvault!')
    except Exception as e:
        print(
            f'Webhook secrets could not be loaded from Azure keyvault!')
        print(e)
    
    # Send teams message
    sendTeamsAlert(failed, webhook, incomingwebhook)

    logging.info('Python timer trigger function ran at %s', utc_timestamp)
