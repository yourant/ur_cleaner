import lazop


appkey = '100692'
app_secret = 'SyqajhckYEjNHe77LOnFvgpXOiGJ6Csg'

client = lazop.LazopClient(f"https://auth.lazada.com/rest", f"{appkey}", f"{app_secret}")
request = lazop.LazopRequest('/auth/token/create')
request.add_api_param('code', '0_100692_jPT1Rh8azmUSvGFpkY8qD5784749')
response = client.execute(request)
print(response)