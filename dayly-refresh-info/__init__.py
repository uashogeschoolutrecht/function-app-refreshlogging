import datetime
import logging
from .functions.authentication import getAccessToken
from .functions.funcs import REFRESHDATASET
from .functions.funcs import sendTeamsAlert
from .functions.funcs import sendTeamsMessage
import azure.functions as func

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

    # DEF for reading from database
    def readFromDB(SERVER, DATABASE, SQLQUERY, USERNAME, PASSWORD):
        import pypyodbc
        connection_string = (
            # Function apps gebruikt op dit moment python 3.10 en die ondersteund ODBC 18 niet vandaar dat deze naar 17 is gezet.
            # Let op bij updaten naar python 3.11 dat de driver naar 18 moet. 
            'DRIVER={ODBC Driver 17 for SQL Server};' 
            f'SERVER={SERVER};'
            f'DATABASE={DATABASE};'
            f'UID={USERNAME};'
            f'PWD={PASSWORD};'
            'TrustServerCertificate=yes'
        )

        # setup connection
        cnxn = pypyodbc.connect(connection_string)
        cursor = cnxn.cursor()
        
        # load data from query
        cursor.execute(SQLQUERY) 
        rows = cursor.fetchall()

        # transform to df
        import pandas as pd
        df = pd.DataFrame.from_records(rows, columns=[x[0] for x in cursor.description])

        return df

    #COUNT ROWS IN D_SECURIRY TABLE
    dsec_count = readFromDB(
        SERVER='DBND27.medewerkers.ad.hvu.nl',
        DATABASE='P_DM',
        SQLQUERY= 'SELECT COUNT(*) AS aantal FROM D_SECURITY',
        USERNAME = os.getenv('bodsproduser'),
        PASSWORD = os.getenv('bodsprodpassword')
        )
    dsec_count = dsec_count['aantal'][0]
    
    #SEND MESSAGE TO TEAM CHANNEL IF ROWS ARE ABOVE 100
    if dsec_count > 100:
        sendTeamsMessage(
            message_name='D_SECURITY', 
            message_content=f'Tabel bevat {dsec_count} rijen HR dashboard security werkt' , 
            webhook=webhook, 
            incomingwebhook=incomingwebhook
            )
        
    # # SEND ERROR MESSAGE TO TEAM CHANNEL IF ROWS
    else:
        sendTeamsMessage(
            message_name='D_SECURITY', 
            message_content=f'WARNING!!! Tabel bevat {dsec_count} rijen HR dashboard security werk niet CHECK MET BACKEND!!' , 
            webhook=webhook, 
            incomingwebhook=incomingwebhook
            )

    logging.info('Python timer trigger function ran at %s', utc_timestamp)
