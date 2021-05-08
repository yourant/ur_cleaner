import lazop



appkey = '100692'
app_secret = 'mqFavnDL6YggaossNRnq4ZXwTshkYsNj'

client = lazop.LazopClient(f"https://auth.lazada.com/rest", f"{appkey}", f"{app_secret}")
request = lazop.LazopRequest('/auth/token/create', 'POST')
request.add_api_param('code', '100692_3g1GABUMKlH60M80JAhZ5wVY13132')
response = client.execute(request)
print(response)