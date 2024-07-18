import urllib
from urllib.request import Request, urlopen
from fake_useragent import UserAgent
import random
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from datetime import datetime

class base():
    def __init__(self):
        self.url = 'https://finance.yahoo.co.jp/stocks/'
        self.engine = create_engine("postgresql+psycopg2://postgres:yui%403286@localhost/yahoo_finance_stock")

    def catalogue(self):
        ua = UserAgent()
        url = self.url
        req = Request(url)
        req.add_header('User-Agent',ua.random)
        web = urlopen(req).read().decode('utf8')
        soup = BeautifulSoup(web,'html.parser')

        anch = soup.find_all('a',class_='_1RUX5ji2')

        url_list = []
        index_list = []
        i = 0
        while i < len(anch):
            url_list.append(anch[i].get('href'))
            i += 1
        
        i = 0
        while i < len(anch):
            index_list.append(anch[i].get_text())
            i += 1

        #df = pd.DataFrame({'Index': index_list, 'URL':url_list})
        page_list = []
        i = 0
        while i < len(url_list):
            url2 = url_list[i]+'&page=1'
            req = Request(url2)
            req.add_header('User-Agent',ua.random)
            web = urlopen(req).read().decode('utf8')
            soup = BeautifulSoup(web,'html.parser')
            div = soup.find_all('div',class_='_3UifNu3u')
            for item in div:
                page_list.append(item.find('p').get_text())
            
            i += 1
        
        df = pd.DataFrame({'Index':index_list,'URL':url_list,'Page':page_list})

        #インデックス事Url_Page数
        no_of_url_page = [1,1,9,7,3,2,11,4,1,1,3,3,2,5,12,13,5,3,6,2,4,1,1,2,31,16,18,5,3,1,2,8,28]
        df['No_of_Urlpage'] = no_of_url_page
        
        #掲載企業数
        no_of_pub = [12,6,164,130,50,25,214,78,10,18,57,43,35,92,227,242,90,49,108,25,64,11,6,38,617,315,352,84,42,14,38,148,552]
        df['no_of_pub'] = no_of_pub

        return df
    
    def profile(self,index,pages):
        engine = self.engine
        query = '''SELECT * FROM yahoo_finance;'''
        base = pd.read_sql(query,engine)
        
        #https://finance.yahoo.co.jp/quote/1301/profileを抽出、List化
        #i = 0 
        #while i < len(base):
        #    for i2 in range(1,base['No_of_Urlpage'])
        #水産・農林業でサンプル
        count = 0
        for i in range(1,pages):
            url = base['url'].loc[index] +f'&page={i}'
            ua = UserAgent()
            req = Request(url)
            req.add_header('User-Agent',ua.random)
            web = urlopen(req).read().decode('utf8')
            soup = BeautifulSoup(web,'html.parser')

            li = soup.find_all('li',class_='_17an0kaG') 

            i2 = 0
            head_list = []
            while i2 < len(li):
                for item in li[i2].find_all('h2',class_='_1ApM7LhG'):
                        head_list.append(item.get_text())
            
                i2 += 1
        
            profile_list = []
            i3 = 0
            while i3 < len(li):
                for item in li[i3].find_all('a',href=True):
                    if 'profile' in item['href']:
                        profile_list.append(item['href'])
                i3 += 1
            
            count += 1

        #会社名、Profile/URLでDataFrame Tableを作成
            df = pd.DataFrame({'index':base['category'].loc[index],'company':head_list,'profile':profile_list})
            

            return df
    
    #Index, Page数を入力してProfile URLを抽出
    def profile2(self,index,pages):
        
        engine = self.engine
        query = '''SELECT * FROM yahoo_finance;'''
        base = pd.read_sql(query,engine)
        
        count = 0
        url_list = []
        for i in range(1,pages+1):
            url_list.append(base['url'].loc[index-1] +f'&page={i}')
            count += 1
        
        soup_list = []
        for item in url_list:
            ua = UserAgent()
            req = Request(item)
            req.add_header('User-Agent',ua.random)
            web = urlopen(req).read().decode('utf8')
            soup = BeautifulSoup(web,'html.parser')
            soup_list.append(soup)
        
        tag_list1 = []
        for item in soup_list:
            tag_list1.append(item.find_all('li',class_='_17an0kaG'))
        
        head_list = []
        i = 0
        while i < len(tag_list1):
            for item in tag_list1[i]:
                for item2 in item.find_all('h2',class_='_1ApM7LhG'):
                    head_list.append(item2.get_text())
            
            i += 1
        
        profile_list = []
        i = 0
        while i < len(tag_list1):
            for item in tag_list1[i]:
                for item2 in item.find_all('a',href=True):
                    profile_list.append(item2['href'])
            
            i += 1
        
        profile_list2 = []
        for item in profile_list:
            if 'profile' in item:
                profile_list2.append(item)


        df = pd.DataFrame({'index':base['category'].loc[index-1],'company':head_list,'profile':profile_list2})
               
        return df

    #index, page数を引数で渡して時価総額を抽出
    def market_cap(self,index,pages):

        engine = self.engine
        query = '''SELECT * FROM yahoo_finance;'''
        base = pd.read_sql(query,engine)
        
        count = 0
        url_list = []
        for i in range(1,pages+1):
            url_list.append(base['url'].loc[index-1] +f'&page={i}')
            count += 1
        
        soup_list = []
        for item in url_list:
            ua = UserAgent()
            req = Request(item)
            req.add_header('User-Agent',ua.random)
            web = urlopen(req).read().decode('utf8')
            soup = BeautifulSoup(web,'html.parser')
            soup_list.append(soup)
        
        tag_list1 = []
        for item in soup_list:
            tag_list1.append(item.find_all('li',class_='_17an0kaG'))
        
        head_list = []
        i = 0
        while i < len(tag_list1):
            for item in tag_list1[i]:
                for item2 in item.find_all('h2',class_='_1ApM7LhG'):
                    head_list.append(item2.get_text())
            
            i += 1
        
        #時価総額情報の抽出/
        mc_list = []
        i = 0
        while i < len(tag_list1):
            for item in tag_list1[i]:
                    target = item.find_all('span')
                    i2 = 0
                    while i2 < len(target):
                        if target[i2].attrs['class'] == ['_3rXWJKZF']:
                                if target[i2].get_text() == '---':
                                    if '0' not in mc_list:
                                        mc_list.append('0')
                                    else:
                                        pass
                                    
                        elif target[i2].attrs['class'] == ['_3rXWJKZF', '_1NrnBlaN']:
                                mc_list.append(target[i2].get_text())
                        else:
                            pass

                        i2 += 1

                #for item2 in item.find_all('span',class_='_3rXWJKZF _1NrnBlaN'):
                            
            i += 1
        mc_list2 = []
        for item in mc_list:
            mc_list2.append(item.replace(',',''))

        df = pd.DataFrame({'index':base['category'].loc[index-1],'company':head_list, 'market_cap':mc_list2})  
        df['market_cap'] = df['market_cap'].astype('int')
        df['unit'] = '百万円'
        df['mc_date'] = datetime.today().strftime('%Y-%m-%d')

        return df
    
    def profile_list(self):
        engine = self.engine
        query = '''SELECT * FROM yahoo_finance;'''
        df = pd.read_sql(query,engine)
        df['no_of_urlpage'] = df['no_of_urlpage'].astype('int')
        df['no_of_pub'] = df['no_of_pub'].astype('int')

        profile_list = []
        i = 0
        while i < len(df):
            if i == 27:
                pass
            else:
                profile_list.append(self.profile2(i,df['no_of_urlpage'].loc[i]+1))

            i += 1
        
        df = pd.concat(profile_list)
        df2 = df.drop_duplicates()

        return df2
    
    #一度に全てloopするとBlockされるため、indexを引数として渡してindex毎にTableを作る(Ex.'その他製品')
    def profile_table(self,category):
        engine = self.engine
        query = '''SELECT * FROM profile_url;'''
        df = pd.read_sql(query,engine)

        df2 = df[df['index']==f'{category}']

        #soup_list
        soup_list = []
        for item in df2['profile_url']:
            ua = UserAgent()
            req = Request(item)
            req.add_header('User-Agent',ua.random)
            web = urlopen(req).read().decode('utf8')
            soup = BeautifulSoup(web,'html.parser')
            soup_list.append(soup)
        
        #会社名/attrs_listは「その他金融業」で抽出。他業種は確認が必要=>建設業もok
        h2 = []
        attrs_list = ['_6uDhA-ZV','_1lc03-BR','_2snKgL4v']
        i = 0
        while i < len(soup_list):
            for item in attrs_list:
                for item in soup_list[i].find_all('h2',class_=item):
                    h2.append(item.get_text())
            i += 1
        
        #企業情報
        td = []
        i = 0
        while i < len(soup_list):
            for item in soup_list[i].find_all('td',class_='_1Y-2cHme'):
                td.append(item.get_text())
            i +=1

        #企業情報Column
        th = []
        for item in soup_list[0].find_all('th',class_='_1VSUDp4T'):
                th.append(item.get_text())
        
        
        #tdをh2/企業名の数に合わせ等分に分割
        td_list = np.array_split(td,len(h2))

        d = {}
        i = 0
        while i < len(td_list):
            for item in td_list:
                d[i] = pd.DataFrame(td_list[i]).transpose()
            i += 1
        
        df = pd.DataFrame(pd.concat(d.values()))
        df.columns = th
        df2 = df.reset_index(drop=True)
        df2['company'] = h2

        return df2
    
    #既に作成したTableの従業員数、給与等stringをintegerへ変換
    def profile_revise(self,table):
        engine = self.engine
        query = f'SELECT * FROM {table};'

        df = pd.read_sql(query,engine)

        #<employee1>
        df['employee1'].replace('---','0',inplace=True)

        #adjust1='人'を削除
        adj1 = []
        for item in df['employee1']:
            if '人'in item:
                adj1.append(item.replace('人',''))
            else:
                adj1.append(item)
        
        #decimal separator','の削除
        adj2 = []
        for item in adj1:
            if ',' in item:
                adj2.append(item.replace(',',''))
            else:
                adj2.append(item)
        
        #string to integerへの変換
        adj3 = [eval(i) for i in adj2]

        df['employee1'] = adj3

        #<employee2>

        df['employee2'].replace('---','0',inplace=True)

        #adjust1='人'を削除
        adj1 = []
        for item in df['employee2']:
            if '人'in item:
                adj1.append(item.replace('人',''))
            else:
                adj1.append(item)
        
        #decimal separator','の削除
        adj2 = []
        for item in adj1:
            if ',' in item:
                adj2.append(item.replace(',',''))
            else:
                adj2.append(item)
        
        #string to integerへの変換
        adj3 = [eval(i) for i in adj2]

        df['employee2'] = adj3

        #<avg_sal>

        df['avg_sal'].replace('---','0',inplace=True)

        #adjust1='千円'を削除
        adj1 = []
        for item in df['avg_sal']:
            if '千円'in item:
                adj1.append(item.replace('千円','000'))
            else:
                adj1.append(item)
        
        #decimal separator','の削除
        adj2 = []
        for item in adj1:
            if ',' in item:
                adj2.append(item.replace(',',''))
            else:
                adj2.append(item)
        
        #string to integerへの変換
        adj3 = [eval(i) for i in adj2]

        df['avg_sal'] = adj3

        #<avg_age>

        df['avg_age'].replace('---','0',inplace=True)

        #adjust1='歳'を削除
        adj1 = []
        for item in df['avg_age']:
            if '歳'in item:
                adj1.append(item.replace('歳',''))
            else:
                adj1.append(item)
        
        #decimal separator','の削除
        adj2 = []
        for item in adj1:
            if ',' in item:
                adj2.append(item.replace(',',''))
            else:
                adj2.append(item)
        
        #string to integerへの変換
        adj3 = [float(i) for i in adj2]

        df['avg_age'] = adj3

        
        return df
    
    def profile_revise2(self,table):
        engine = self.engine
        query = f'SELECT * FROM {table};'

        df = pd.read_sql(query,engine)
        #<avg_age>

        df['avg_age'].replace('---','0',inplace=True)

        #adjust1='歳'を削除
        adj1 = []
        for item in df['avg_age']:
            if '歳'in item:
                adj1.append(item.replace('歳',''))
            else:
                adj1.append(item)
        
        #decimal separator','の削除
        adj2 = []
        for item in adj1:
            if ',' in item:
                adj2.append(item.replace(',',''))
            else:
                adj2.append(item)
        
        #string to integerへの変換
        adj3 = [float(i) for i in adj2]

        df['avg_age'] = adj3

        return df
    


        
        
    