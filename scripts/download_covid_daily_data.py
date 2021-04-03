import requests
import pandas as pd
import datetime
departements = pd.read_csv("data/pop/fr/departements-francais.csv", sep = ";")
hospi = []
url = "https://coronavirusapi-france.now.sh/AllLiveData"
response = requests.get(url).json()
counter = 0
for numero in departements.NUMÉRO:
    nom = str(departements[departements["NUMÉRO"]==numero]["NOM"].values[0])
    hospi.append((nom, numero, response["allLiveFranceData"][counter]["nouvellesHospitalisations"]))
    counter+=1

df = pd.DataFrame(hospi, columns =["depname","depnum","newhospi"])
print(df)
df.to_csv("covid_daily_data.csv", index = False)