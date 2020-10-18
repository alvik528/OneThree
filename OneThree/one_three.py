import psycopg2
from bs4 import BeautifulSoup
import requests
import cfscrape
import csv

#Postgresql parameters
DATABASE = "postgres"
USER = 'postgres'
PASSWORD = 'password'

#Here we are defining the drug_id's to be used for this program, this list can be modified depending on the testing criteria
drugbank_id = ['DB00619', 'DB01048', 'DB14093', 'DB00173', 'DB00734', 'DB00218', 'DB05196',
'DB09095','DB01053', 'DB00274']

#With the current configuration of how we are creation our actions object, this dictionary 
#is what we are using to map the actions to values and indexes
actions_dict = {'Antagonist':2, 'Multitarget':3, 'Inhibitor':4, 'Binder':5, 'Substrate':6, 'Inducer':7}

#This decoding is necessary due to our scraping of smiles, this replaces the current encrypted text with values surrounding an @ sigh
#this error is occuring due to the CloudFlare email protection picking up the '@' as an email
def cfDecodeEmail(encodedString):
    r = int(encodedString[:2],16)
    email = ''.join([chr(int(encodedString[i:i+2], 16) ^ r) for i in range(2, len(encodedString), 2)])
    return email

#This is our function to return a list of alternate identifiers as specified by the alternate identifiers table,This function
#takes a string for the DrugBank ID and a soup parameter of the HTML content for the page and returns a 2 dimensional list
#of size [n][4]
def alternative_identifiers(m,soup):
    final = []
    try:
        h2 = soup.find(id="external-links").find_next("dd")
        sources = h2.find_all('dt')
        id_links = h2.find_all('dd')
        for i in range (0,len(sources)):
            final.append([m, sources[i].text, id_links[i].text, id_links[i].a['href']])
    except():
        print('There was an error with getting hte identifier for' +  m)
    return final

#This function takes in html content and returns the full SMILES code, there is necessary decrpytion that needs to be done due to cloudflare email encryption
def get_smiles(soup):
    h2=soup.find("dt", string="SMILES")
    p_after_h2=h2.find_next("dd")
    x = p_after_h2.find('div',class_='wrap')
    if (x): # Check to see if we are grabbing a valid SMILES value
        encrypted_list = x.text.split('[email\xa0protected]')
        encryptions = x.find_all('a')
        for i in range(0,len(encryptions)):
            encrypted_list[i] = encrypted_list[i] + cfDecodeEmail(encryptions[i]['data-cfemail'])
        return ''.join(encrypted_list)
    return None

#This is our function to return a list of all of the targets and actions that are available for a given Drug,
#this function takes in a DrugBank ID as well as html content and returns a 2 dimensional array of size [n][6]
def get_actions_targets(m,soup):
    final = []
    try:
        #this structure would need to be abstracted for other data sources
        h2 = soup.find(id="targets").find('div',attrs = {'class': 'bond-list'}).find_all('div',attrs = {'class': 'bond card'})
        for item in h2:
            actions_targets = [m,'None',False,False,False,False,False,False]
            #these tags would need to be abstracted for other data sources
            actions = item.find_all('div',attrs = {'class': 'badge badge-pill badge-action'})
            for i in actions:
                actions_targets[actions_dict[i.text]] = True
            gene_name = item.find("dt", string="Gene Name")
            if(gene_name):
                gene_valid = gene_name.find_next("dd").text
                actions_targets[1] = gene_valid
            final.append(actions_targets)
    except AttributeError:
        print('We are sorry we could not grab your current value')  # If there is an attribute error or we can't grab the current value 
    return final

'''
NOTE AND DISCLAIMER MOVING FOWARD WITH THE DATABASE PROMPT
: Due to having to work on my work computer and not currently having a personal device, I am limited on the ability to download Postgresql and getting
it to run due to needing admin permissions. Becuase of this, although I have included code and conjecture as to what I think would properly run, the database
portion of this assignment has not been run and is not meant to run.
'''
def create_db():
    #establishing the connection
    conn = psycopg2.connect(
    database=DATABASE, user=USER, password=PASSWORD, host='127.0.0.1', port= '5432'
    )
    conn.autocommit = True

    cursor = conn.cursor()

    sql = '''CREATE database DrugBank''';

    cursor.execute(sql)
    print("Database created successfully........")

    #Closing the connection
    conn.close()

def create_tables():
    commands = (
        """
        CREATE TABLE Drugs (
            drug_identifier_id SERIAL PRIMARY KEY,
            DrugBank_nm VARCHAR(50) PRIMARY KEY,
            Smiles VARCHAR(255) NOT NULL
        )
        """,
        """ CREATE TABLE Targets (
                drug_target_id SERIAL PRIMARY KEY,
                FOREIGN KEY (DrugBank_ID)
                    REFERENCES vendors (DrugBank_ID)
                    ON UPDATE CASCADE ON DELETE CASCADE,
                target_nm VARCHAR(255),
                antagonist boolean,
                multitarget boolean,
                inhibitor boolean,
                binder boolean,
                substrate boolean,
                inducer boolean
                
            )
        """,
        """
        CREATE TABLE Identifiers (
                drug_identifier_id SERIAL PRIMARY KEY,
                FOREIGN KEY (DrugBank_ID)
                    REFERENCES vendors (DrugBank_ID)
                    ON UPDATE CASCADE ON DELETE CASCADE,
                identifier_src nvarchar(255),
                identifier_id nvarchar(255),
                identifier_link nvarchar(255)
        )
        """)
    conn = psycopg2.connect(
    database=DATABASE, user=USER, password=PASSWORD, host='127.0.0.1', port= '5432'
    )
    
    cursor = conn.cursor()

    cursor.execute("DROP TABLE IF EXISTS Drugs")

    cursor.execute("DROP TABLE IF EXISTS Targets")

    cursor.execute("DROP TABLE IF EXISTS Identifiers")

    cursor.execute(commands)
    print("Table created successfully........")
    
    conn.close()

#The insert scripts below can be optimized, into one function with the ability to take varying input, and the queries can be placed in a SQL file to be run
#Unfortunately I have run out of time for this assignment but I wanted to make a note for this as well.
def insert_DrugBank_list(drugbank_list):
    """ insert multiple vendors into the vendors table  """
    sql = "INSERT INTO Drugs(DrugBank_nm,smiles) VALUES(%s,%s)"
    conn = None
    try:
        conn = psycopg2.connect(
    database=DATABASE, user=USER, password=PASSWORD, host='127.0.0.1', port= '5432'
    )
        cur = conn.cursor()
        for i in drugbank_list:
            cur.execute(sql,i[0],i[1])
        conn.commit()
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()

def insert_target_list(target_list):
    """ insert multiple vendors into the vendors table  """
    sql = "INSERT INTO Targets(DrugBank_ID, target_nm ,antagonist, multitarget, inhibitor, binder, substrate, inducer) VALUES(%s,%s,%s,%s,%s,%s,%s,%s)"
    conn = None
    try:
        conn = psycopg2.connect(
    database=DATABASE, user=USER, password=PASSWORD, host='127.0.0.1', port= '5432'
    )
        cur = conn.cursor()
        for i in target_list:
            cur.execute(sql,i[0],i[1],i[2],i[3],i[4],i[5],i[6],i[7])
        conn.commit()
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            
def insert_identifier_list(identifier_list):
    """ insert multiple vendors into the vendors table  """
    sql = "INSERT INTO Identifiers (DrugBank_ID, identifier_src,identifier_id, identifier_link) VALUES(%s,%s,%s,%s)"
    conn = None
    try:
        conn = psycopg2.connect(
    database=DATABASE, user=USER, password=PASSWORD, host='127.0.0.1', port= '5432'
    )
        cur = conn.cursor()
        for i in identifier_list:
            cur.execute(sql,i[0],i[1],i[2],i[3])
        conn.commit()
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            
def write_csv(twod_array, name):
    with open(name+".csv","w+") as my_csv:
        csvWriter = csv.writer(my_csv,delimiter=',')
        csvWriter.writerows(twod_array)
        
#we are creating 3 temprary lists to populate to put into our tables
#if we are running this for multiple thousand items especially at the same time, there should be some parallel processing in place
#to account for this
def main():
    drug = [['DrugBank_ID','DrugBank_nm','SMILES']]
    identifier = [['DrugBank_ID', 'identifier_src', 'identifier_id', 'identifier_link']]
    target = [['DrugBank_ID',	'target_nm',	'antagonist',	'multitarget',	'inhibitor',	'binder',	'substrate',	'inducer']]
    for m in drugbank_id:
        page = requests.get("https://go.drugbank.com/drugs/"+m)
        soup = BeautifulSoup(page.content, 'html5lib')
        drug.append([m,get_smiles(soup)])
        target.extend(get_actions_targets(m,soup))
        identifier.extend(alternative_identifiers(m,soup))
        
    #For the sake of this exercise, to confirm output, I will be writing these three "Tables" to csv values since I am limited on the Postgresql side
    print(drug)
    print(identifier)
    write_csv(drug,"Drug")
    write_csv(identifier,"Identifier")
    write_csv(target,"Target")
    #If any output checking needs 

if __name__ == "__main__":
    main()
    