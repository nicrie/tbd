import requests
import pandas as pd
import datetime
from tqdm import tqdm
# departements = pd.read_csv("data/pop/fr/departements-francais.csv", sep = ";")
# hospi = []
# url = "https://coronavirusapi-france.now.sh/AllLiveData"
# response = requests.get(url).json()
# counter = 0
# for numero in departements.NUMÉRO:
#     nom = str(departements[departements["NUMÉRO"]==numero]["NOM"].values[0])
#     hospi.append((nom, numero, response["allLiveFranceData"][counter]["hospitalises"]))
#     counter+=1

departements = pd.read_csv("data/pop/fr/departements-francais.csv", sep = ";")
hospi = []
url = "https://coronavirusapi-france.now.sh/AllDataByDepartement?Departement="

datapointlist = []
for nom in tqdm(departements.NOM):
    url2 = url + nom
    numero = str(departements[departements["NOM"]==nom]["NUMÉRO"].values[0])
    response = requests.get(url2).json()
    referencedate = datetime.datetime.strptime("2020-04-08", '%Y-%m-%d')
    for i in range(len(response["allDataByDepartement"])):
        date = datetime.datetime.strptime(response["allDataByDepartement"][i]["date"], '%Y-%m-%d')
        if (date > referencedate):
            if "hospitalises" in response["allDataByDepartement"][i].keys():
                hospitalises = response["allDataByDepartement"][i]["hospitalises"]
            else:
                hospitalises = response["allDataByDepartement"][i]["hospitalisation"]
            reanimation = response["allDataByDepartement"][i]["reanimation"]
            nouvellesHospitalisations = response["allDataByDepartement"][i]["nouvellesHospitalisations"]
            nouvellesReanimations = response["allDataByDepartement"][i]["nouvellesReanimations"]
            deces = response["allDataByDepartement"][i]["deces"]
            gueris = response["allDataByDepartement"][i]["gueris"] 
            datapointlist.append((nom,numero,date,hospitalises,reanimation,nouvellesHospitalisations,nouvellesReanimations,deces,gueris))

dfcolumns = ['nom', 'numero','date','hospi','reanim','newhospi','newreanim','deces','gueris']
df = pd.DataFrame(datapointlist, columns = dfcolumns)
df.to_csv('data/Covid_data_history.csv', index = False)
print(df)
