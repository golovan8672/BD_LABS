import pandas as pd
import numpy as np
import re
from lxml import etree

root = etree.parse("OBV_full.xml")

columns = []

vacancies = root.find('vacancies').getiterator('vacancy')
for vacancy in vacancies:
  tags = vacancy.getiterator()
  for i in tags:
    if i.tag not in columns:
      columns.append(i.tag)
      
columns.remove('address')
columns.remove('addresses')
columns.remove('salary')
columns.append('salary_from')
columns.append('salary_to')


columns_with_index = {}
for i in range(len(columns)):
  columns_with_index[columns[i]] = i


def add_value(row, col_name, value):
  index = columns_with_index[col_name]
  if not pd.isna(row[index]):
    row[index] = row[index]+':'+value
  else:
    row[index] = value

n = 0

salary_from = re.compile("от (\d+)", re.IGNORECASE)
salary_to = re.compile("до (\d+)", re.IGNORECASE)

result_rows = []
Row = []
vacancies = root.find('vacancies').getiterator('vacancy')
print('Чтение данных и конвертация')

for vacancy in vacancies:
  if len(Row)!=0:
    result_rows.append(Row)
  Row = [np.nan] * len(columns)
  tags = vacancy.getiterator()
  n+=1
  if n % 30000 == 0:
    print(f'Обработано {n} записей')
  for i in tags:
    if i.text and not i.text.isspace() and i.tag!='vacancy' and i.tag!='address' and i.tag!='addresses':
      if i.tag == 'job-name':
        add_value(Row, i.tag, i.text.replace(',',';'))
      elif i.tag == 'salary':
        fr=salary_from.search(i.text)
        to=salary_to.search(i.text)
        fr = fr.group(1) if fr else np.nan
        to = to.group(1) if to else np.nan
        add_value(Row, 'salary_from', fr)
        add_value(Row, 'salary_to', to)
      else:
        add_value(Row, i.tag, i.text)
if Row:
  result_rows.append(Row)

print('Запись части данных')
result = pd.DataFrame(result_rows[:200], columns=columns)
print('Запись завершилась успешно')
result.to_csv("Result_part.csv",  na_rep = '*')
print('Запись всех данных')
result = pd.DataFrame(result_rows, columns=columns)
result.to_csv("Result_full.csv",  na_rep = '*')
print('Запись завершилась успешно')