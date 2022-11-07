#!/usr/bin/env python
# coding: utf-8

# In[34]:


import pandas as pd
import numpy as np
get_ipython().run_line_magic('matplotlib', 'inline')
import matplotlib.pyplot as plt
import seaborn as sns
import plotly.express as px
import statistics
import datetime
import requests
from urllib.parse import urlencode


# In[35]:


customers='https://disk.yandex.ru/d/FUi5uSd6BfG_ig'
orders='https://disk.yandex.ru/d/t9Li4JOfgxuUrg'
products='https://disk.yandex.ru/d/Gbt-yAcQrOe3Pw'


# In[36]:


base_url = 'https://cloud-api.yandex.net/v1/disk/public/resources/download?'
public_key_1=customers
final_url_1 = base_url + urlencode(dict(public_key=public_key_1))
response = requests.get(final_url_1)
download_url_1 = response.json()['href']


# In[37]:


base_url = 'https://cloud-api.yandex.net/v1/disk/public/resources/download?'
public_key_2=orders
final_url_2 = base_url + urlencode(dict(public_key=public_key_2))
response = requests.get(final_url_2)
download_url_2 = response.json()['href']


# In[38]:


base_url ='https://cloud-api.yandex.net/v1/disk/public/resources/download?'
public_key_3=products
final_url_3 = base_url + urlencode(dict(public_key=public_key_3))
response = requests.get(final_url_3)
download_url_3 = response.json()['href']


# In[40]:


customers=pd.read_csv(download_url_1, sep=',')
orders=pd.read_csv(download_url_2, sep=',')
products=pd.read_csv(download_url_3, sep=',')


# In[41]:


customers.head(3)


# In[42]:


orders.head(3)


# In[43]:


products.head(3)


# In[44]:


# 1. Сколько у нас пользователей, которые совершили покупку только один раз? 


# In[45]:


orders.head(3)


# In[46]:


orders.order_status.unique()


# In[47]:


""" Я считаю, что стасус 'delivered' будет самым подходящем, так как при данном статусе мы точно знаем, что товар доставлен клиенту в полном объеме, не был потерян,что оплата была совершена, а клиент его принял.При других статусах у нас больше вероятность,что покупка не будет завершена полностью"""


# In[48]:


orders_delivered=orders.query("order_status=='delivered'")
orders_delivered.head(3)


# In[49]:


customers_id=customers[['customer_id','customer_unique_id']]


# In[50]:


orders_delivered_unique_id=orders_delivered.merge(customers_id,on='customer_id',how='left')


# In[51]:


orders_delivered_unique_id.head(3)


# In[52]:


orders_delivered_unique_id.groupby('customer_unique_id', as_index=False)                             .agg({'order_id': 'count'})                              .rename(columns={'order_id':'cnt'})                             .query('cnt == 1').shape


# In[53]:


# Ответ: 90557 пользователей совершили только однку покупу 


# In[ ]:





# In[54]:


#2. Сколько заказов в месяц в среднем не доставляется по разным причинам (вывести детализацию по причинам)


# In[55]:


orders.head(3)


# In[56]:


orders['order_estimated_delivery_date_Month']=pd.to_datetime(orders.order_estimated_delivery_date).dt.to_period("M")


# In[57]:


orders_not_delivery=orders.query("order_status!='delivered'")
orders_not_delivery.head(3)


# In[58]:


orders_not_delivery_status=orders_not_delivery.groupby(['order_estimated_delivery_date_Month','order_status'],as_index=False)     .agg({'order_id':'nunique'})     .rename(columns={'order_id':'not_dilivery_cnt'})     .groupby('order_status',as_index=False)     .agg({'not_dilivery_cnt':'mean'})     .round()
orders_not_delivery_status


# In[64]:


ax=sns.barplot(data=orders_not_delivery_status, x='order_status',y='not_dilivery_cnt')
sns.set(rc={'figure.figsize':(12,6)})
ax.set_ylabel('not_dilivery_cnt\n')
ax.set_xlabel('\norder_status')


# In[65]:


""" Я считаю, что самые главные статусы canceled и unavailable, так как по дргуим статусам доставка будет осуществлена позже либо заявка перейдет в другой статус"""


# In[66]:


# Ответ: По cтатусу unavailable 29 и по cтатусу canceled 24 заказов в месяц в среднем не доставляется соотвественно 


# In[ ]:





# In[67]:


# 3. По каждому товару определить, в какой день недели товар чаще всего покупается. (7 баллов)


# In[68]:


orders_products=orders_delivered.merge(products,on='order_id',how='left')
orders_products.head(3)


# In[69]:


orders_products['day_of_the_week']=pd.to_datetime(orders_products.order_purchase_timestamp).dt.day_name()
orders_products[['day_of_the_week','product_id','order_id']].head(3)


# In[70]:


orders_products_day_week=orders_products.groupby(['product_id', 'day_of_the_week'],as_index=False)                 .agg({'order_id': 'count'})                 .rename(columns={'order_id':'cnt'})
orders_products_day_week.head(3)


# In[71]:


orders_products_day_week.pivot(index='product_id', columns='day_of_the_week', values='cnt')            .idxmax(axis=1)             .to_frame()             .reset_index()             .rename(columns={0:'most_popular_day'})


# In[ ]:





# In[72]:


"""4. Сколько у каждого из пользователей в среднем покупок в неделю (по месяцам)? Не стоит забывать, что внутри месяца может быть не целое количество недель. Например, в ноябре 2021 года 4,28 недели. И внутри метрики это нужно учесть."""


# In[73]:


customers_buys=orders.merge(customers, how='left', on='customer_id').query("order_status=='delivered'")
customers_buys.head(3)


# In[74]:


customers_buys_month=customers_buys.groupby(['order_purchase_timestamp', 'customer_unique_id'], as_index=False)                           .agg({'order_id': 'count'})                           .rename(columns={'order_id': 'orders_month'})
customers_buys_month.head(3)


# In[75]:


customers_buys_month['week']=(pd.to_datetime(customers_buys_month.order_purchase_timestamp).dt.daysinmonth)/7
customers_buys_month.head(3)


# In[76]:


customers_buys_month=customers_buys_month.assign(orders_week=customers_buys_month.orders_month/customers_buys_month.week)
customers_buys_month.head(3)


# In[77]:


customers_buys_month[['customer_unique_id','orders_week']]


# In[ ]:





# In[101]:


"""""5. Используя pandas, проведи когортный анализ пользователей. В период с января по декабрь выяви когорту с самым высоким     retention на 3й месяц."""


# In[110]:


cohorts = orders.merge(customers, how='left', on='customer_id')
cohorts.head(3)


# In[134]:


orders.order_purchase_timestamp=pd.to_datetime(orders.order_purchase_timestamp)
cohorts['order_month']=orders.order_purchase_timestamp.dt.to_period("M")


# In[150]:


cohorts.set_index('customer_unique_id', inplace=True)
cohorts['join_month'] = cohorts.groupby(level=0)['order_purchase_timestamp'].min().apply(lambda x: x.strftime('%Y-%m'))
cohorts.reset_index(inplace=True)
cohorts.insert(len(cohorts.columns), 'orders_num', 0, allow_duplicates=False)
cohorts.head()


# In[151]:


# формируем когорты
cohorts_1 = cohorts.groupby(['join_month', 'order_month'])                    .agg({'customer_unique_id': pd.Series.nunique, 'orders_num': pd.Series.count})                    .rename(columns={'customer_unique_id': 'total_customers'})
cohorts_1.head()


# In[152]:


def CohortPeriod(C):
    C['cohort_period'] = np.arange(len(C)) + 0
    return C
cohorts_1 = cohorts_1.groupby(level=0).apply(CohortPeriod)
cohorts_1.head()


# In[153]:


cohorts_1.reset_index(inplace=True)
cohorts_1.set_index(['cohort_period', 'join_month'], inplace=True)
cohorts_1.head()


# In[154]:


cohort_group_size = cohorts_1['total_customers'].groupby(level=1).first()
cohorts_1['total_customers'].unstack(0)


# In[155]:


retention_rate = cohorts_1['total_customers'].unstack(0).divide(cohort_group_size, axis=0)
retention_rate


# In[156]:


cohort_analysis = (retention_rate
            .style
            .set_caption('User retention by cohort') 
            .background_gradient(cmap='icefire')
            .highlight_null('white') 
            .format("{:.2%}", na_rep=""))
cohort_analysis


# In[ ]:


#Ответ: со значением retention rate в 0.41% самый высокий показатель у когорты '2017-06'.


# In[ ]:




