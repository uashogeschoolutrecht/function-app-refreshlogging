# Refresh logging Power BI datasets

## Goal
The goal of this application is to give updates on the refresh status of the Power BI datasets. This is done with a python script that both reads from the Power BI tenant as well as posts messages to the Data & Analytics teams channel. The application is deployed to Azure as a function app.  


### Setting up the Function App
Before writing the application we need to setup a [function app](https://learn.microsoft.com/en-us/azure/azure-functions/functions-overview?pivots=programming-language-python) where the application can be deployed. Essentially a function app is a piece of script (in this case a python script) that is stored on an Azure server and can be trigger in several ways (in our case by a time trigger). 

![image](dayly-refresh-info\images\trigger-flow.png)

The app `pbi-refresh-logging` is created on the `HU-PLG-DWH-DataPlatfrom` resource on the DENA Azure portal. In Visual studio code a connection is made to this app and from there a local template is created. The Azure functions extension has a variety of templates to choose from, one of which is the python time trigger template. This gives you the necessary files to get started. Two of which are most important, the `function.json` file and the `__init__.py file`. The former is used to set the time trigger and the latter is the file that runs the python script. Once we set this up, we can start writing our code!

### Getting dataset info
As stated, the goal of this app is to get daily info on whether or not the refresh of the datasets were successful. In order to do this, we first we need to get the information from the HU Power BI tenant thru the API. A service principal has been added to the Power BI tenant that is used to retrieve this information. In order use this service principal we need to get an access token from the Microsoft server. In the authentication folder we have a function that retrieves this token. 

```python
def getAccessToken(resource, client_id, client_secret, domain):
    '''get acces token from the powerbi security group that was added to the tenant admin'''
    auth_url  = f'https://login.microsoftonline.com/{domain}'
    import adal
    context = adal.AuthenticationContext(authority=auth_url,
                                         validate_authority=True,
                                         api_version=None)
    token = context.acquire_token_with_client_credentials(
        resource, client_id, client_secret)

    # Get acces token
    access_token = token.get('accessToken')

    return access_token

```

Both the `clientId` as the `clientSecret` have been added to the to `KV-DENA` key vault. These are necessary to get access to the service principal. Since we do not want to use these secret identifiers to be part of the script, they need to be read from the vault directly. In order to do this we need to give our function access to the vault.

### Adding Vault to Function App
In order to give our function access to the vault we need to manually add this access in the Azure portal. First go to the DENA vault and go to the Access policies tab on the left and click on create.
![image](images\acces-policies.png)
Select get and list from the Secret permissions pane, this will allow the app to read from the vault.
![image](images\get-list.png)
Selecte the app you created from the principal list.
![image](images\select-principal.png)
The app now has access to the vault. You need to add access to specific secrets however. Browse to the vault secret you would like to use (in this case we need the clientId and the clientSecret from the Power BI API service principal) and click on the current version.
![image](images\vault-clientId.png)
Copy the secret identifier and keep it in your clipboard. Browse to your function app in the azure portal and go to the configurations pane. 
![image](images\config-pane-function.png)
click on application setting, in the popup enter under name the name of the secret (in this case the clientId) and under value type `@Microsoft.KeyVault(SecretUri=<paste-from-clipboard.)` and paste the url that you just copied.
![image](images\application-setting.png)
Follow the same steps for the clientSecret. Both have now been added to the function app and can be read from your python script. 


### Writing the python code
Now that we have can get access to the vault we can retrieve the secrets from the vault in our python scripts. This works similar as retrieving them from your environmental variables from your local pc. 

```python
import os 
clientId = os.getenv('clientId')
clientSecret = os.getenv('clientSecret')
```
Now that you have access to the API you can start writing your python script. Right now the only thing we want to know is whether or not a refresh has been successful, the following python class checks for this.
```python

# Create refresh class
class REFRESHDATASET:
    '''This class loads the workspace and dataset Id's for the dataset that needs to be refreshed. It triggers a refresh retrieves 
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
    

    def getRefreshInfo(self):
        '''Load latest refresh info on the dataset'''
        output = pd.DataFrame()
        for id, name in zip(self.datasetIds, self.datasetNames):
            uri = f"https://api.powerbi.com/v1.0/myorg/groups/{self.workspaceId}/datasets/{id}/refreshes"
            headers = {'Authorization': f'Bearer {self.accessToken}'}
            response = requests.get(uri, headers=headers)
            if response.status_code == 200:
                response = json.loads(response.content)
                res = pd.concat([pd.json_normalize(x) for x in response['value']])
                res.sort_values(by='startTime', ascending=False, inplace=True)
                res.reset_index(drop=True, inplace=True)
                res = res.iloc[0]
                results = pd.DataFrame({'datasetname': [name], 'status': [
                                    res['status']], 'lastrefresh': [res['startTime']]})
            elif response.status_code == 415:
                results = pd.DataFrame({'datasetname': [name], 'status': [
                                    'Direct query, no refresh info'], 'lastrefresh': ['n.v.t.']})
            else:
                continue
            output = pd.concat([output, results], ignore_index=True)
            output.reset_index(drop=True,inplace=True)

        return output


```
The two input variables form the class are the accesstoken (which can be retrieved with the definition that was show earlier) and the workspace name. Right now, we only want to check de D&A prod data sources workspace. What the script does is check all dataset ID’s and names that are in the specified workspace. It takes these Id's and checks the refresh log from the following endpoint: `https://api.powerbi.com/v1.0/myorg/groups/{workspaceId}/datasets/{datasetId}/refreshes`. It returns a data frame with info on all those datasets. This information is stored and will later be used to send out a teams message.

### Post messages to Team channel
Now that we have our refresh information, we can send a message to the team’s channel. In order to do this, we need to set up a connection with the Webhook application in Microsoft teams (you need to request access with the IT department). Please check the [documentation](https://learn.microsoft.com/en-us/microsoftteams/platform/webhooks-and-connectors/how-to/add-incoming-webhook?tabs=dotnet) on how to setup the webhook. 

Once you have set this up you have a connection to teams, with the following python code you can send updates:

```python
def sendTeamsAlert(dataframe):       
    # import libraries here
    bot_url = '<enter your bot url from the webhook here>'
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
                        "activityTitle": "Daily fresh",
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
                    "activityTitle": "Daily fresh",
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
```




### Deploying the function app with time trigger 

Now that your app is completed in can be deployed to the function on Azure. Before we do this however we need to set the time, this is done with a [cron expression](https://en.wikipedia.org/wiki/Cron#CRON_expression) in the `function.json` file on the schedule line. A cron expression is a string with 6 separate expressions which represent a given schedule via patterns. The pattern we use to represent every 5 minutes is `0 */5 * * * *`. This, in plain text, means: "When seconds is equal to 0, minutes is divisible by 5, for any hour, day of the month, month, day of the week, or year". Our app is set to run every weekday at 7 in the morning. 

