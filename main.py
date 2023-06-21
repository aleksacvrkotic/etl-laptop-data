import requests
import numpy as np
import pandas as pd
from bs4 import BeautifulSoup
from sqlalchemy import create_engine
#Declaring all the columns


def get_data():
    names = []
    prices = []
    eans = []
    screen_diags = []
    rams = []
    processors = []
    graphics = []
    memorys = []
    base_link  = 'https://gigatron.rs'
    #Number of pages You want to scrape
    for i in range(1,5):


        links = []
        true_links = []
        print(f'Strting to scrape page number {i}...')
        #Here i is representing the page number so we can plug that in our url
        url = 'https://gigatron.rs/prenosni-racunari/laptop-racunari/?strana=' + str(i)
        # Setting up the headres so we look more like a human
        headers = {
            'authority': 'search.gigatron.rs',
            'method':'GET',
            'path': '/v1/catalog/get/prenosni-racunari/laptop-racunari?strana=2',
            'scheme': 'https',
            'accept': 'application/json, text/plain, */*',
            'accept-encoding': 'gzip, deflate, br',
            'accept-language': 'sr-RS,sr;q=0.9,en-US;q=0.8,en;q=0.7,hu;q=0.6,hr;q=0.5,pl;q=0.4',
            'origin': 'https://gigatron.rs',
            'referer': 'https://gigatron.rs/',
            'sec-ch-ua': '"Chromium";v="112", "Google Chrome";v="112", "Not:A-Brand";v="99"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
            'sec-fetch-dest': 'empty',
            'sec-fetch-mode': 'cors',
            'sec-fetch-site': 'same-site',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.0.0 Safari/537.36'
            }
        #Getting our response for the ulr we provided
        response = requests.get(url)
        soup = BeautifulSoup(response.content, 'html.parser')
        #Getting all the product so we can get all the links
        products = soup.find_all('div', class_='item')
        #Getting the links
        for product in products:
            link = product.find('a', href=True)['href']
            links.append(link)
        links = list(set(links))
        for link in links:
            link = base_link + link
            true_links.append(link)

        true_links = list(set(true_links))
        #Getting data form every product on the page
        for link in true_links:
            response = requests.get(link, headers=headers)
            soup = BeautifulSoup(response.content,  'html.parser')
            ean = soup.select('#content > div.wrap.productpage > div > div > div > div > div > div > div.product-main-data.clear > div.col.col-7.col-size-product-top.noselect.w60 > ul > li:nth-child(1) > span:nth-child(2) > span')
            #Eliminating the duplicates
            if ean[0].text in eans:
                continue
            try:
                price = soup.find('span', class_='ppra_price-number snowflake').text
                prices.append(price)
            except:
                try:
                    price = soup.find('span', class_='ppra_price-number').text
                    prices.append(price)
                except:
                    price = np.nan
                    prices.append(price)
            try:
                name = soup.select('#content > div.wrap.productpage > div > div > div > div > div > div > div.product-main-data.clear > div.col.col-4.w30 > div > div > div.col.col-12 > div > h1')
                names.append(name[0].text)
            except:
                name = np.nan
                names.append(name)

            try:
                ean = soup.select('#content > div.wrap.productpage > div > div > div > div > div > div > div.product-main-data.clear > div.col.col-7.col-size-product-top.noselect.w60 > ul > li:nth-child(1) > span:nth-child(2) > span')
                eans.append(ean[0].text)
            except:
                ean = np.nan
                eans.append(ean)

            try:
                screen_diag = soup.select('#content > div.wrap.productpage > div > div > div > div > div > div > div.product-main-data.clear > div.col.col-7.col-size-product-top.noselect.w60 > ul > li:nth-child(3) > span:nth-child(2)')
                screen_diags.append(screen_diag[0].text)
            except:
                screen_diag = np.nan
                screen_diags.append(screen_diag)

            try:
                ram = soup.select('#content > div.wrap.productpage > div > div > div > div > div > div > div.product-main-data.clear > div.col.col-7.col-size-product-top.noselect.w60 > ul > li:nth-child(5) > span:nth-child(2)')
                rams.append(ram[0].text)
            except:
                ram = np.nan
                rams.append(ram)

            try:
                proccesor = soup.select('#content > div.wrap.productpage > div > div > div > div > div > div > div.product-main-data.clear > div.col.col-7.col-size-product-top.noselect.w60 > ul > li:nth-child(2) > span:nth-child(2)')
                processors.append(proccesor[0].text)
            except:
                proccesor = np.nan
                processors.append(proccesor)

            try:
                graphic = soup.select('#content > div.wrap.productpage > div > div > div > div > div > div > div.product-main-data.clear > div.col.col-7.col-size-product-top.noselect.w60 > ul > li:nth-child(4) > span:nth-child(2)')
                graphics.append(graphic[0].text)
            except:
                graphic = np.nan
                graphics.append(graphic)

            try:
                memory = soup.select('#content > div.wrap.productpage > div > div > div > div > div > div > div.product-main-data.clear > div.col.col-7.col-size-product-top.noselect.w60 > ul > li:nth-child(6) > span:nth-child(2)')
                memorys.append(memory[0].text)
            except:
                memory = np.nan
                memorys.append(memory)
        print(f'On this page I have scraped {len(true_links)} items!')


        #Formating the data to a dictionary
    data = {
        'name' : names,
        'ean': eans,
        'screen_diag': screen_diags,
        'ram': rams,
        'processor': processors,
        'graphic': graphics,
        'Storage': memorys,
        'price': prices
    }
    #converting the data to pandas DataFrame
    df = pd.DataFrame(data)
    #Saving the data to a csv file
    #df.to_csv(r'C:\Users\Cvrle\Desktop\sve Aleksa\PROJEKTI\ON GOING PROJEKTI\ETL_1\Data\Laptop_Data_Raw.csv', index=False)
    return df

def clean_data():
    df = get_data()


    df.dropna(inplace=True)




    df['Brand_Name'] = df['name'].str.split().str[0]
    columns = ['Brand_Name'] + list(df.columns[:-1])
    df = df[columns]
    df.drop('name', axis=1, inplace=True)



    df.drop('ean', axis=1, inplace=True)



    #SCREEN DIAGONAL column
    df['screen_diag(in)'] = df['screen_diag'].str.replace('"', '')
    df['screen_diag(in)'] = df['screen_diag(in)'].str.replace("''", '')
    df['screen_diag(in)'] = df['screen_diag(in)'].astype(float)


    df.drop('screen_diag', axis=1, inplace=True)


    df['ram'] = df['ram'].str.replace('GB', '')
    df['ram'] = df['ram'].astype(int)
    df = df.rename(columns={'ram': 'RAM(GB)'})



    df[['Processor', 'Frequency']] = df['processor'].str.split(' do ', expand=True)
    df['Brand'] = 'Other'  # Default value for all rows
    df.drop('processor', axis=1, inplace=True)
    df.loc[df['Processor'].str.contains('Intel'), 'Brand'] = 'Intel'
    df.loc[df['Processor'].str.contains('AMD'), 'Brand'] = 'AMD'



    df['Frequency'] = df['Frequency'].str.replace('GHz', '')
    df['Frequency'] = df['Frequency'].astype(float)
    df = df.rename(columns={'Frequency': 'Frequency(GHz)'})



    df[['Storage_size', 'Storage_Unit']] = df['Storage'].str.split(n=1, expand=True)
    df.drop('Storage' , axis=1, inplace=True)



    df['Integrated'] = df['graphic'].str.contains('Integrisana').astype(int)
    df['graphic'] = df['graphic'].str.replace('Integrisana ', '')



    df['graphic_brand'] = df['graphic'].str.split().str[0]


    df['Storage_size'] = df['Storage_size'].str.replace('GB', '')
    df['Storage_size'] = df['Storage_size'].str.replace('TB', '')
    df['Storage_size'] = df['Storage_size'].astype(int)
    df.loc[df['Storage_size'] > 50, 'Storage_size'] /= 1024
    df = df.rename(columns={'Storage_size': 'Storage_size(TB)'})



    df['price'] = df['price'].astype(float)
    df['price'] = (((df['price'] * 1000)-1) * 0.0093).astype(float).round()



    return df



def load_data(df):
    engine = create_engine('sqlite:///C:/Users/Cvrle/Laptopdatabase.db')

    # Create a table in the database using the DataFrame
    df.to_sql('laptop_data_table', con=engine, if_exists='append', index=False)

    # Close the database connection
    engine.dispose()



clean_df = clean_data()
print('Data Cleaned!')
print(clean_df)
load_data(clean_df)
print('Data Loaded')
