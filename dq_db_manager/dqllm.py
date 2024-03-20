import os,constants
from langchain.prompts import PromptTemplate
from langchain_openai import ChatOpenAI
from langchain.chains.llm import LLMChain

os.environ['OPENAI_API_KEY'] = constants.APIKEY

TIMEOUT = 30
MAX_FAILURE_COUNT = 2

generate_sql_prompt = PromptTemplate(
    input_variables=["query"],
    template="""
    You are a database admin, you can make sql queries to your database
    by using the following syntax:

    EXECUTE("<QUERY">)

    For example: to get everything from the students table, you should 
    respond as:
    EXECUTE("SELECT * FROM students")

    I will be asking you questions related to my database, you have 
    to give me the flow which you will be following to respond to my query
    Make sure to return the query without any placeholders and use the current database
    use schema as DATABASE()

    Here is my query: {query}
    """
)

parse_sql_data_prompt = PromptTemplate(
    input_variables=["query", "sql", "result"],
    template="""
    I have asked you a {query}, for which you gave me the following sql outpul {sql};
    Here are the result of the sql query:
    {result}

    Summarize the results, don't start the summary like 'the results of the sql queries mean'
    """
)

parse_sql_error_prompt = PromptTemplate(
    input_variables=["sql", "error"],
    template="""
    I am running the following sql query {sql} and getting the following {error},
    Fix the query and only return the fixed query as output
    """
)

class DQLLM:
    def __init__(self, query, execute_function):
        self.execute_function = execute_function
        self.query = query

    def get_sql_query(self, query):
        chain = LLMChain(llm=ChatOpenAI(verbose=False, timeout=TIMEOUT), prompt=generate_sql_prompt)
        res = chain.invoke(
        input={
            "query": query} 
        )
        sql = res['text']
        return sql[sql.index('("')+2: sql.rindex('")')]

    def parse_sql_data(self, sql_query, sql_query_result):
        chain = LLMChain(llm=ChatOpenAI(verbose=True, timeout=TIMEOUT), prompt=parse_sql_data_prompt)
        res = chain.invoke(
        input={
            "query": self.query,
            "sql": sql_query,
            "result": sql_query_result 
            } 
        )
        return res['text']
    
    def parse_sql_error(self, sql_query, sql_query_error):
        print(f"Parsing SQL ERROR: {sql_query}, {sql_query_error}")
        chain = LLMChain(llm=ChatOpenAI(verbose=True, timeout=TIMEOUT), prompt=parse_sql_error_prompt)
        res = chain.invoke(
        input={
            "sql": sql_query,
            "error": sql_query_error 
            } 
        )
        return res['text']
        

    def run(self):
        sql_query = self.get_sql_query(self.query)
        failure_count = 0
        while True:
            print(f"Executing: {sql_query}")
            res_sql_query = self.execute_function(sql_query)
            print(f"Result: {res_sql_query}")
            if(str(res_sql_query).lower().startswith("error")):
                print("Query Failed, Trying again")
                failure_count+=1
                if failure_count >= MAX_FAILURE_COUNT:
                    break
                sql_query = self.parse_sql_error(sql_query, res_sql_query)  
            else:
                break  
        print(self.parse_sql_data(sql_query, res_sql_query))



  