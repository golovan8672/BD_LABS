import pandas as pd
import numpy as np
import urllib.request, json 
import csv
import dateutil.parser
from datetime import datetime, date, time
import re
import os
import shutil
import time

import threading

listDelimiter = ';'

retryDelayS = 0.5

threadCount = 20

mapColumns = [
        {
            'column' : 'Name',
            'json_name' : 'name',
            'dtype' : 'object',
            'multiple' : False
        },
        {
            'column' : 'City',
            'json_name' : 'address.city',
            'dtype' : 'object',
            'multiple' : False
        },
        {
            'column' : 'Salary from',
            'json_name' : 'salary.from',
            'dtype' : 'float',
            'multiple' : False
        },
        {
            'column' : 'Salary to',
            'json_name' : 'salary.to',
            'dtype' : 'float',
            'multiple' : False
        },
        {
            'column' : 'Employer name',
            'json_name' : 'employer.name',
            'dtype' : 'object',
            'multiple' : False
        },
        {
            'column' : 'Published at',
            'json_name' : 'published_at',
            'dtype' : 'string',
            'multiple' : False
        },
        {
            'column' : 'Expierence',
            'json_name' : 'experience.name',
            'dtype' : 'object',
            'multiple' : False
        },
        {
            'column' : 'Employment',
            'json_name' : 'employment.name',
            'dtype' : 'object',
            'multiple' : False
        },
        {
            'column' : 'Schedule',
            'json_name' : 'schedule.name',
            'dtype' : 'object',
            'multiple' : False
        },
        {
            'column' : 'Description',
            'json_name' : 'description',
            'dtype' : 'object',
            'multiple' : False
        },
        {
            'column' : 'Responsibility',
            'json_name' : 'snippet.responsibility',
            'dtype' : 'object',
            'multiple' : False
        },
        {
            'column' : 'Requirement',
            'json_name' : 'snippet.requirement',
            'dtype' : 'object',
            'multiple' : False
        },
        {
            'column' : 'Key skills',
            'json_name' : 'key_skills.name',
            'dtype' : 'object',
            'multiple' : True
        }
    ]

parsedIds = []

resultRows = []

def getUrlJson(url):
    while(True):
        try:
            response = urllib.request.urlopen(url)
            return json.loads(response.read().decode("utf-8"))
        except:
            time.sleep(retryDelayS)

def getPages(specialization):
    curentPage = 0
    totalPages = 1
    while(curentPage<totalPages):
        page = getUrlJson("https://api.hh.ru/vacancies?specialization="+specialization+"&per_page=140&page="+str(curentPage))
        totalPages = page["pages"]
        curentPage += 1
        yield page

def getVacancyThread(lst, vacancies):
    for vacancy in vacancies:
        lst.append([vacancy,getUrlJson("https://api.hh.ru/vacancies/"+str(vacancy['id']))])

def split(a, n):
    k, m = divmod(len(a), n)
    return (a[i * k + min(i, m):(i + 1) * k + min(i + 1, m)] for i in range(n))

def getVacanciesDetails(listOfVacancies):
    parts = list(split(listOfVacancies, threadCount))
    results = []
    threads = []
    for i in range(len(parts)):
        results.append([])
        thread = threading.Thread(target=getVacancyThread, args=(results[i],parts[i],))
        threads.append(thread)
        thread.start()
    for thread in threads:
        thread.join()

    return [item for sublist in results for item in sublist]


def getValue(column, value, multiple=""):
    if column['multiple']:
        result = ""
        for item in value:
            result += str(item[multiple]) + listDelimiter
        result = result[:-1]
        if result == "":
            result = np.nan
    else:
        if value == None:
            result = np.nan
        else:
            result = str(value)
    return result

def addVacancyToRow(vacancy, details):
    row = []
    #print(vacancy)
    for column in mapColumns:
        names = column['json_name'].split('.')
        if len(names) == 1:
            if names[0] in vacancy and vacancy[names[0]] != None:
                row.append(getValue(column, vacancy[names[0]]))
            elif names[0] in details and details[names[0]] != None:
                row.append(getValue(column, details[names[0]]))
            else:
                row.append(np.nan)
        else:
            if column['multiple']:
                if names[0] in vacancy and vacancy[names[0]] != None:
                    row.append(getValue(column, vacancy[names[0]], names[1]))
                elif names[0] in details and details[names[0]] != None:
                    row.append(getValue(column, details[names[0]], names[1]))
                else:
                    row.append(np.nan)
            else:
                if names[0] in vacancy and vacancy[names[0]] != None and names[1] in vacancy[names[0]]:
                    row.append(getValue(column, vacancy[names[0]][names[1]]))
                elif names[0] in details and details[names[0]] != None and names[1] in details[names[0]]:
                    row.append(getValue(column, details[names[0]][names[1]]))
                else:
                    row.append(np.nan)
    return row

def getVacancies():
    specializations = getUrlJson("https://api.hh.ru/specializations")
    spec = 0
    for specialization in specializations[0]['specializations']:
        specC += 1
        print("Специализация "+str(specC)+" из "+str(len(specializations[0]['specializations'])))
        pageC = 0
        for page in getPages(specialization['id']):
            vacancies = []
            for vacancy in page["items"]:
                if vacancy['id'] not in parsedIds: #and vacancy['id'] == '33386754'
                    vacancies.append(vacancy)
                    parsedIds.append(vacancy['id'])
            data = getVacanciesDetails(vacancies)
            for vacancyData in data:
                row = addVacancyToRow(vacancyData[0], vacancyData[1])
                resultRows.append(row)
            pageC += 1
            print("   Страница "+str(pageC))        
        #break

csvColumns = []
types = {}
for col in mapColumns:
    csvColumns.append(col['column'])
    types[col['column']] = col['dtype']

getVacancies()

dt = pd.DataFrame(resultRows, columns=csvColumns)

dt = dt.astype(types)

dt.to_csv("temp.csv",  na_rep = 'NA', index=True, index_label="", quotechar='"', quoting=csv.QUOTE_NONNUMERIC, encoding="utf-8-sig")

dt = dt.sort_values(["Salary to", "Salary from"])

shutil.rmtree('./Salaries', ignore_errors=True)
shutil.rmtree('./Vacancies', ignore_errors=True)
os.mkdir('./Salaries')
os.mkdir('./Vacancies')

def saveCount(df, col, file):
    row = []
    for item in df[col].unique():
        if not pd.isna(item):
            count = (df[col] == item).sum()
            row.append([item, count])
    newDf = pd.DataFrame(row, columns=[col, "Count"])
    newDf.to_csv(file,  na_rep = 'NA', index=True, index_label="", quotechar='"', quoting=csv.QUOTE_NONNUMERIC, encoding="utf-8-sig")

def saveCount2(df, col1, col2, file):
    row = []
    for item1 in df[col1].unique():
        eq = df.loc[(df[col1] == item1)]
        for item in eq[col2].unique():
            if not pd.isna(item):
                count = (eq[col2] == item).sum()
                row.append([item1, item, count])
    newDf = pd.DataFrame(row, columns=[col1, col2, "Count"])
    newDf.to_csv(file,  na_rep = 'NA', index=True, index_label="", quotechar='"', quoting=csv.QUOTE_NONNUMERIC, encoding="utf-8-sig")


#ЧАСТЬ 2

for column in dt.columns:
    if str(dt.dtypes[column]) == 'object':
        dt[column] = dt[column].apply(lambda x: x.lower() if not pd.isna(x) else np.nan)
dt["Published at"] = dt["Published at"].apply(lambda x: dateutil.parser.parse(x, ignoretz=True))

notNullSalary = dt.loc[(dt['Salary to'].notnull()) & (dt['Salary from'].notnull())]
salariesGroups = np.array_split(notNullSalary['Salary to'].unique(),10)
lastSalary = 0
daysRows = []

for salaryGroup in salariesGroups:
    group = notNullSalary.loc[(notNullSalary['Salary from'] > lastSalary) & (notNullSalary['Salary to'] <= max(salaryGroup))]
    salaryGroupS = str(int(lastSalary)) + "-" + str(int(max(salaryGroup)))
    lastSalary = max(salaryGroup)
    os.mkdir("./Salaries/" + salaryGroupS)
    saveCount(group, "Name", "./Salaries/" + salaryGroupS + "/Names.csv")
    publishedAt = group["Published at"]
    if len(publishedAt)>0:
        publishedAt = (datetime.now() - publishedAt).dt.days
        daysRows.append([salaryGroupS, publishedAt.mean(), publishedAt.min(), publishedAt.max()])
    else:
        daysRows.append([salaryGroupS, np.nan, np.nan, np.nan])
    saveCount(group, "Expierence", "./Salaries/" + salaryGroupS + "/Expierence.csv")
    saveCount(group, "Employment", "./Salaries/" + salaryGroupS + "/Employment.csv")
    saveCount(group, "Schedule", "./Salaries/" + salaryGroupS + "/Schedule.csv")    
    skills = group.loc[group["Key skills"].notnull()]
    if len(skills) != 0 :
        skills = pd.DataFrame(skills)
        skills["Key skills"] = skills["Key skills"].apply(lambda x: x.split(listDelimiter))
        skills = skills.explode("Key skills")
        saveCount(skills, "Key skills", "./Salaries/" + salaryGroupS + "/KeySkills.csv")

sDaysresult = pd.DataFrame(daysRows, columns=['Range', "Avg", "Min", "Max"])
sDaysresult.to_csv("./Salaries/Days.csv",  na_rep = 'NA', index=True, index_label="", quotechar='"', quoting=csv.QUOTE_NONNUMERIC, encoding="utf-8-sig")

#ЧАСТЬ 2

saveCount2(dt, "Name", "Salary from", "./Vacancies/Salary from.csv")
saveCount2(dt, "Name", "Salary to", "./Vacancies/Salary to.csv")
saveCount2(dt, "Name", "Expierence", "./Vacancies/Expierence.csv")
saveCount2(dt, "Name", "Employment", "./Vacancies/Employment.csv")
saveCount2(dt, "Name", "Schedule", "./Vacancies/Schedule.csv")

skills = dt.loc[dt["Key skills"].notnull()]
skills = pd.DataFrame(skills)
skills["Key skills"] = skills["Key skills"].apply(lambda x: x.split(listDelimiter))
skills = skills.explode("Key skills")
saveCount2(skills, "Name", "Key skills", "./Vacancies/KeySkills.csv")
