import os
from langchain_groq import ChatGroq
from langchain_core.prompts import PromptTemplate
from dotenv import load_dotenv

load_dotenv()

class Chain:
    def __init__(self):
        
        self.llm = ChatGroq(
            temperature=0,
            groq_api_key=os.getenv("GROQ_API_KEY"),
            model_name="llama-3.3-70b-versatile"
        )

    def explain_block(self, sql_docs: list, user_prompt: str, chat_history: list = None):

        """
        sql_docs: list of dicts with keys 'sql' and 'meta'
        user_prompt: current question
        chat_history: list of previous interactions, each dict with 'user_prompt' and 'llm_response'
        
        """

        history_text = ""
        if chat_history:
            for entry in chat_history:
                history_text += f"User asked: {entry['user_prompt']}\nAssistant answered: {entry['llm_response']}\n\n"

        if not sql_docs or len(sql_docs) == 0:
            return "No relevant SQL scripts found."

        script_context = ""
        for doc in sql_docs:
            sql, meta = doc["sql"], doc["meta"]
            script_context += f"""
            --- SCRIPT: {meta.get("script_name")}
            Schema: {meta.get("schema")}
            Block: {meta.get("block")}
            Dialect: {meta.get("dialect", "PostgreSQL")}
            Inputs: {meta.get("inputs", [])}
            Outputs: {meta.get("outputs", [])}
            Order: {meta.get("order")}
            Dependencies: {meta.get("dependencies", [])}

            SQL:
            {sql}
            """

        prompt_sql = PromptTemplate.from_template(
            """
            ### CONTEXT:
            You are an expert SQL assistant. Your task is to explain SQL templates, business rules, and dimensional logic clearly and accurately.

            ### SCRIPTS:
            {scripts}

            ### USER REQUEST:
            {user_request}

            ### INSTRUCTION:
            - Analyze all the retrieved scripts and proper result for the user request.
            - Describe the logic, dependencies, and how fields or logic (like churn/upsell) propagate.
            """
        )

        chain_sql = prompt_sql | self.llm
        res = chain_sql.invoke({
            "scripts": script_context,
            "user_request": user_prompt
        })

        return res.content
    
    def generate_scripts(self, sql_docs: list, user_prompt: str, chat_history: list = None):
        """
        sql_docs: list of dicts with keys 'sql' and 'meta'
        user_prompt: current question
        chat_history: list of previous interactions, each dict with 'user_prompt' and 'llm_response'
        
        """
        history_text = ""
        if chat_history:
            for entry in chat_history:
                history_text += f"User asked: {entry['user_prompt']}\nAssistant answered: {entry['llm_response']}\n\n"

        if not sql_docs or len(sql_docs) == 0:
            return "No relevant SQL scripts found."
        

        script_context = ""
        for doc in sql_docs:
            sql, meta = doc["sql"], doc["meta"]
            script_context += f"""
            --- SCRIPT: {meta.get("script_name")}
            Schema: {meta.get("schema")}
            Block: {meta.get("block")}
            Dialect: {meta.get("dialect", "PostgreSQL")}
            Inputs: {meta.get("inputs", [])}
            Outputs: {meta.get("outputs", [])}
            Order: {meta.get("order")}
            Dependencies: {meta.get("dependencies", [])}

            SQL:
            {sql}
            """
        
        prompt_sql = PromptTemplate.from_template("""
            ### ROLE:
            You are an expert SQL engineer. Your job is to update an existing SQL template strictly according to the user request.

            ### TEMPLATE SQL (CONTEXT):
            {scripts}

            ### USER REQUEST:
            {user_request}

            ### REQUIRED OUTPUT:
            Return ONLY the updated SQL script.
                                                
            ### Flow of the Scripts:
            1. Monthly Revenue
            2. Customer Contract
            3. Customer Product Contract
            4. Period Revenue
            5. Customer Lifecycle Events
            6. Customer Product Lifecycle Events
            7. Delta Revenue                      

            ### STRICT RULES:
            1. Provide all the necessary updated scripts by proper flow.
            2. Explain the code at the script level not the block level.
            3. If the user request adds dimensions, update SELECT, GROUP BY, JOIN, and WHERE consistently.
            4. Maintain all naming conventions and formatting exactly as in the original template.
            5. Do NOT add new tables or fields unless explicitly requested.
            """)

        chain_sql = prompt_sql | self.llm
        res = chain_sql.invoke({
            "scripts": script_context,
            "user_request": user_prompt
        })

        return res.content