import requests

#input for the facility number - this will be replaced
Facility_Number = input("Insert Facility number here: ")
#"14916"
#API_KEY
API_KEY = "Y7aR2DB4.heXriM0CyBgXICzrSlT9jdfkWHY3CYjl"
if API_KEY is None:
    print("issue is with key")
    exit() 

#Base URL
URL = f"https://www.peeringdb.com/api/ix/{Facility_Number}"

# Headers
headers = {"Authorization": f"Api-Key {API_KEY}"}
print(headers)

# Making the GET Request
#Required_Fields ="id, org_name,website,net_set,fac_set,carrier_set,ix_set,campus_set,address1,campus_set,address2,city,country,state,zipcode,latitude,longitude,name"

response = requests.get(URL, headers=headers)

#Check if response is successful
if response.status_code== 200:
    data = response.json()["data"][0]
    print ("Facility Data:", data)
else:
    print("failed to get data", response.status_code)


 # columns needed

# 'id':
# 'org_name'
# 'website'
# 'net_set'
# 'fac_set'
# 'carrier_set'
# 'ix_set'
# 'campus_set'
# 'address1':
# 'campus_set':
# 'address2': '',
# 'city':',
# 'country':
# 'state': 'CA',
# 'zipcode': '
# 'latitude': 37.527344,
# 'longitude': -122.261179,
# 'name'