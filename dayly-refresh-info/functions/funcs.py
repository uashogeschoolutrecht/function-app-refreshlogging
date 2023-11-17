# ignore warnings
import pandas as pd
import json
import requests
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)
warnings.simplefilter(action='ignore', category=UserWarning)


class bcolors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKCYAN = '\033[96m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'


# Create refresh class
class REFRESHDATASET:
    '''This class loads the workspace and dataset Id's for the dataset that needs to be refreshed. It triggers a refresh reftrieves 
   latest refresh data'''

    def __init__(self, accessToken, workspaceName):
        self.accessToken = accessToken
        self.workspaceName = workspaceName
        self.workspaceId = self.getDatasetIds()[0]
        self.datasetIds = self.getDatasetIds()[1]
        self.datasetNames = self.getDatasetIds()[2]

    def getDatasetIds(self):
        '''Get the workspace ID based on the workspace name from which you would like te refresh the dataset'''
        # Set base URL
        base_url = 'https://api.powerbi.com/v1.0/myorg/'
        header = {'Authorization': f'Bearer {self.accessToken}'}

        # Get workspaces with all downstream things
        uri = f"{base_url}admin/groups?$filter=endswith(name,'{self.workspaceName}')&?$filter type eq 'Workspace'&$expand=datasets&$top=5000"

        # HTTP GET Request
        groups = requests.get(uri, headers=header)

        # convert to json in order to retrieve workpsace ID
        if groups.status_code == 200:
            groups = json.loads(groups.content)
            result = pd.concat([pd.json_normalize(x) for x in groups['value']])
            datasetIds = list(
                pd.concat([pd.json_normalize(x) for x in result['datasets']])['id'])
            datasetNames = list(
                pd.concat([pd.json_normalize(x) for x in result['datasets']])['name'])
        else:
            print(f'''{bcolors.FAIL}Workspace could not be found please check the workspace name!!
CAPITAL LETTER SENSITIVE!!
If error persists please contact system administrator!!''')
            exit()

        if len(groups['value']) > 1:
            print(
                f'{bcolors.FAIL}Multiple workspaces selected please verify if the name is correct!!')
            exit()
        else:
            # get dataset IDs
            return groups['value'][0]['id'], datasetIds, datasetNames

    def postRefreshDataset(self):
        # refresh dataset workspaceId = self.getWorkspaceId
        for id, name in zip(self.datasetIds, self.datasetNames):
            uri = f"https://api.powerbi.com/v1.0/myorg/groups/{self.workspaceId}/datasets/{id}/refreshes"
            headers = {'Authorization': f'Bearer {self.accessToken}'}
            response = requests.post(uri, headers=headers)

            # List for failed dataset refreshes
            failed_refresh = []
            dq_refresh = []
            # Check the response status code
            if response.status_code == 202:
                print(
                    f'{bcolors.OKGREEN}Dataset: {name} refresh initiated successfully.')
            elif response.status_code == 415:
                print(
                    f'{bcolors.WARNING}Dataset: {name} is a Directquery dataset and does not need to be refreshed!')
                dq_refresh += [name]
            else:
                print(f'{bcolors.FAIL}Failed to initiate dataset refresh for {name}')
                failed_refresh += [name]

        return failed_refresh, dq_refresh

    def getRefreshInfo(self):
        '''Load latest refresh info on the dataset'''
        output = pd.DataFrame()

        for id, name in zip(self.datasetIds, self.datasetNames):
            uri = f"https://api.powerbi.com/v1.0/myorg/groups/{self.workspaceId}/datasets/{id}/refreshes"
            headers = {'Authorization': f'Bearer {self.accessToken}'}
            response = requests.get(uri, headers=headers)
            if response.status_code == 200: # Import dataset refresh types
                response = json.loads(response.content)
                res = pd.concat([pd.json_normalize(x) for x in response['value']])
                res.sort_values(by='startTime', ascending=False, inplace=True)
                res.reset_index(drop=True, inplace=True)
                res = res.iloc[0]
                results = pd.DataFrame({'datasetname': [name], 'status': [
                                    res['status']], 'lastrefresh': [res['startTime']]})
            elif response.status_code == 415: # Direct query dataset refresh types
                results = pd.DataFrame({'datasetname': [name], 'status': [
                                    'Direct query, no refresh info'], 'lastrefresh': ['n.v.t.']})
            else: # Other types of dataset refreshes, e.g. no refresh
                results = pd.DataFrame()
                continue
            output = pd.concat([output, results], ignore_index=True)
            output.reset_index(drop=True,inplace=True)

        return output



def sendTeamsAlert(dataframe,webhook,incomingwebhook):       
    # import libraries here
    bot_url = f"https://hogeschoolutrecht.webhook.office.com/webhookb2/{webhook}/IncomingWebhook/{incomingwebhook}"
    headers = {
        'Content-Type': 'application/json'
    }
    if len(dataframe) > 0 :
        for i in dataframe.index: 
            payload = json.dumps({
                "@type": "MessageCard",
                "@context": "http://schema.org/extensions",
                "themeColor": "0076D7",
                "summary": "Daily fresh",
                "sections": [
                    {
                        "activityTitle": "Daily refresh",
                        "activitySubtitle": "",
                        "activityImage": "",
                        "facts": [
                            {
                            "name": dataframe['datasetname'][i],
                            "value": dataframe['status'][i]
                            }
                        ],
                        "markdown": True
                    }

                ]
            }
            )

            response = requests.request("POST", bot_url, headers=headers, data=payload)
            print(response.text)
    else:
        payload = json.dumps({
            "@type": "MessageCard",
            "@context": "http://schema.org/extensions",
            "themeColor": "0076D7",
            "summary": "Daily fresh",
            "sections": [
                {
                    "activityTitle": "Daily refresh",
                    "activitySubtitle": "",
                    "activityImage": "",
                    "facts": [
                        {
                        "name": "All datasets",
                        "value": "Refreshed succesfull"
                        }
                    ],
                    "markdown": True
                }

            ]
        }
        )

        response = requests.request("POST", bot_url, headers=headers, data=payload)
        print(response.text)
    

def sendTeamsMessage(message_name,message_content,webhook,incomingwebhook):
    bot_url = f"https://hogeschoolutrecht.webhook.office.com/webhookb2/{webhook}/IncomingWebhook/{incomingwebhook}"
    headers = {
        'Content-Type': 'application/json'
    }
    payload = json.dumps({
        "@type": "MessageCard",
        "@context": "http://schema.org/extensions",
        "themeColor": "0076D7",
        "summary": "Daily fresh",
        "sections": [
            {
                "activityTitle": "Daily refresh",
                "activitySubtitle": "",
                "activityImage": "",
                "facts": [
                    {
                    "name": f"{message_name}",
                    "value": f"{message_content}"
                    }
                ],
                "markdown": True
            }

        ]
    }
    )

    response = requests.request("POST", bot_url, headers=headers, data=payload)
    print(response.text)


def readFromDB(USERNAME, PASSWORD, SERVER, DATABASE, SQLQUERY):
    '''This function is used to read from a database and returns a table'''
    from sqlalchemy.engine import URL
    from sqlalchemy import create_engine
    import pandas as pd
    import sqlalchemy as sa

    # get connection URL
    connection_url = URL.create(
        'mssql+pyodbc',
        query={
            'odbc_connect': 'Driver={ODBC Driver 18 for SQL Server};'
            f'Server={SERVER};'
            f'Database={DATABASE};'
            'TrustServerCertificate=yes;'
            f'UID={USERNAME};'
            f'PWD={PASSWORD};'
        },
    )

    engine = create_engine(connection_url)

    with engine.begin() as conn:
        df = pd.read_sql_query(sa.text(SQLQUERY), conn)

    return df
