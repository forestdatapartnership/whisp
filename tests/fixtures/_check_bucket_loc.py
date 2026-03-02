import ee

ee.Initialize(project="ee-andyarnellgee")
from google.auth.transport.requests import AuthorizedSession

creds = ee.data.get_persistent_credentials()
session = AuthorizedSession(creds)
resp = session.get("https://storage.googleapis.com/storage/v1/b/whisp_bucket")
d = resp.json()
print("Bucket:", d.get("name"))
print("Location:", d.get("location"))
print("Storage class:", d.get("storageClass"))
print("Location type:", d.get("locationType"))
