#Description:
#python -m main --clear_sql_database --clear_doc_store --make_csv --ticker=AAPL --years_back=5
#This project extracts 10k filings from the SEC website. It saves them in pdf form and parses them to find their respective balance sheets. Normalized balance sheet data is saved to a sql database (persistantly). After this process is complete, a chatbot allows you to ask questions about the data; the chatbot uses sql and the existing database to run queries and answer the questions
#You have the option to specify what company you would like to analyze, and how many years back you would like to see
#You have the option to clear whatever was persistantly stored to the sql and pdf-document databases
#This project will represent an instance for a specific user


#assumptions:
#for publically traded companies (identifiable by tickers)
#only extracting balance sheet
#only extracting x number of 10ks in the consecutive past (which is given as an input)
#doens't cache sql queries yet
#data is stored locally
#openai api is a key called OPENAI_API_KEY in the .env file
#isn't 100% portable yet (no docker setup yet)
#print statements are logs
#just one company to deal with at a time
#only works on companies that have actual spreadsheet fields as balance sheets (ie google, apple, etc.)... more code is needed for msft
#gpt 4o is the used model




#To Do
#improve extract_table_near_page function
#make sure api doesn't get 1 more or 1 less pdf than input
#augement the pdf file name immediately after downloading it
#add tests (pytest)
#remove redundant imports
#add comments
#make clear_pdf_store part of utils and generate_uid also in utils + other stuff
#allow for tolerance of many companies at once (remove clear all each time there is a switch of companies and add more to few shot learning)... query for input ticker before running interactive bot
#make into api (and adject argparse stuff to be api call arguments)
#make docker file
#add logging in seperate directory
#put graphing mechanism (ie bokeh dashboard)
#add a vector db + RAG
#ensure all paths are using absolute path wrt home directory of project
#provide option to edit the output sql code




