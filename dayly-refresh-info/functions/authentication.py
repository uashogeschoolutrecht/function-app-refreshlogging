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


