import pandas
import re
import csv
import dateutil.parser
import datetime

df = pandas.read_csv("Lab4.csv", delimiter = ",", index_col = [0], na_values = ['NA'], low_memory = False)

allVariants = [
        ['frontend', 'front-end', 'front end', 'фронтенд', 'фронтэндер'],
        ['backend', 'back-end', 'back end', 'бэкенд'],
        ['developer', 'разработчик'],
        ['programmer', 'программист'],
        ['game designer', 'гейм дизайнер', 'геймдизайнер'],
        ['designer', 'дизайнер'],
        ['copywriter', 'копирайтер'],
        ['manager', 'менеджер'],
        ['animator', 'аниматор'],
        ['artist', 'художник'],
        ['middle','миддл'],
        ['js', 'javascript'],
        ['web','вэб'],
        ['teamlead','тимлид', 'team lead', 'team leader'],
        ['devops', 'девопс'],
        ['fullstack','full стек','full-стек','full stack'],
        ['бд','баз данных','баз даных','базы данных'],
        ['2d','2д'],
        ['3d','3д'],
        ['bitrix','битрикс'],
        ['analyst','аналитик'],
        ['big data','bigdata'],
        ['tech lead','techlead'],
        ['mssql','ms sql'],
        ['presale','pre sale', 'presales', 'pre sales']
    ]

delimiters = ['/','+',' ']

comb = ['c++','c#','php','asp.net','ui','ux','qt','sql','delphi','vuejs','angular','unix','linux','java','typescript','win','kotlin','golang','python','node.js','backend','frontend','junior', 'middle', 'senior','go','html5','html','reactjs','ios','android']

delete = ['удаленно','remote','full-time','full time','фултайм','fulltime', '(\(.*\))','(\[.*\])','\s+в\s+.*','г\.\s+.*']

df["Name"] = df["Name"].apply(lambda x: x.lower())
df["Name"] = df["Name"].apply(lambda x: x.replace('\\','/'))
df["Name"] = df["Name"].apply(lambda x: x.replace('|','/'))
df["Name"] = df["Name"].apply(lambda x: x.replace(',','/'))
df["Name"] = df["Name"].apply(lambda x: x.replace('-',' '))
df["Name"] = df["Name"].apply(lambda x: re.sub('\s*\/\s*', '/', x))
df["Name"] = df["Name"].apply(lambda x: re.sub('\s+', ' ', x).strip())

for d in delete:
    regex = re.compile(d)
    df["Name"] = df["Name"].apply(lambda x: regex.sub('', x))

for v in allVariants:
    for i in range(1, len(v)):
        df["Name"] = df["Name"].apply(lambda x: x.replace(v[i],v[0]))

for i in range(len(lang)):
    for j in range(i+1, len(lang)):
        for d in delimiters:
            regex = re.compile(re.escape(lang[j])+'\s*'+re.escape(d)+'\s*'+re.escape(lang[i]))
            df["Name"] = df["Name"].apply(lambda x: regex.sub(lang[i]+delimiters[0]+lang[j], x))

df["Name"] = df["Name"].apply(lambda x: re.sub('\s+', ' ', x).strip())

df["Employer name"] = df["Employer name"].fillna("Не указано")
df["City"] = df["City"].fillna("Не указан")
df["Expierence"] = df["Expierence"].fillna("Не требуется")
df["Employment"] = df["Employment"].fillna("Любой тип")
df["Schedule"] = df["Schedule"].fillna("Любой график")
df["Responsibility"] = df["Responsibility"].fillna("Нету")
df["Requirement"] = df["Requirement"].fillna("Нету")
df["Key skills"] = df["Key skills"].fillna("Нету")

df["Salary from"] = df.groupby(["Name", "City"]).transform(lambda x: x.fillna(x.mean()))["Salary from"]
df["Salary to"] = df.groupby(["Name", "City"]).transform(lambda x: x.fillna(x.mean()))["Salary to"]

df["Published at"] = df["Published at"].apply(lambda x: (datetime.datetime.now() - dateutil.parser.parse(x).replace(tzinfo=None)).days)

df.to_csv("lab4.csv",  na_rep = 'NA', index = True, index_label = "", quotechar = '"', quoting = csv.QUOTE_NONNUMERIC, encoding = "utf-8-sig")
