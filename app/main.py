import time
import datetime
import chromadb
from chromadb.utils.embedding_functions import SentenceTransformerEmbeddingFunction
import streamlit as st
from chains import Chain
from concurrent.futures import ThreadPoolExecutor
from rag import RAG

def create_streamlit_app():
    st.title("📊 Meet MR. CBR – Your Snowball Assistant")
    st.caption("Ask me about churn, upsell, dependencies, or flow across CRB scripts.")

    mode = st.radio("Mode", ["Explanation", "Generation"])

    user_prompt = st.chat_input("Ask a question...")

    if user_prompt:
        # Rate-limit
        now = datetime.datetime.now()
        prev_time = st.session_state.get("prev_request_time", datetime.datetime.fromtimestamp(0))
        if now - prev_time < MIN_TIME_BETWEEN_REQUESTS:
            time.sleep((MIN_TIME_BETWEEN_REQUESTS - (now - prev_time)).seconds)
        st.session_state.prev_request_time = now

        st.chat_message("user").write(user_prompt)

        sql_doc = st.session_state.rag.query_with_flow(user_prompt)
        if sql_doc:
            if mode == 'Generation':
                llm_response = st.session_state.chain.generate_scripts(sql_doc, user_prompt, st.session_state.chat_history)
            else:
                llm_response = st.session_state.chain.explain_block(sql_doc, user_prompt, st.session_state.chat_history)

        else:
            llm_response = "No relevant SQL script found."

        with st.chat_message("assistant"):
            st.write(llm_response)
            for doc in sql_doc:   # Loop through all retrieved scripts
                meta = doc["meta"]
                with st.expander(f"Block: {meta.get('block', 'N/A')} - Description"):
                    st.json(meta) 

        # Save chat
        st.session_state.messages.append({"role": "user", "content": user_prompt})
        st.session_state.messages.append({"role": "assistant", "content": llm_response})
        st.session_state.chat_history.append({
            "user_prompt": user_prompt,
            "llm_response": llm_response,
            "sql_docs": sql_doc
        })


if __name__ == "__main__":
    st.set_page_config(page_title="MR. CBR - CRB Assistant", page_icon="🧠", layout="wide")
    executor = ThreadPoolExecutor(max_workers=5)
    MIN_TIME_BETWEEN_REQUESTS = datetime.timedelta(seconds=2)

    embedding_fn = SentenceTransformerEmbeddingFunction( 
                model_name="BAAI/bge-small-en-v1.5" 
        )
    
    if "chroma_client" not in st.session_state:
        st.session_state.chroma_client = chromadb.PersistentClient("vectorstore_sql")

    st.session_state.sql_collection = (
        st.session_state.chroma_client.get_or_create_collection(
            name="sql_templates",
            embedding_function=embedding_fn
        )
    )

    if "rag" not in st.session_state:
        st.session_state.rag = RAG(st.session_state.sql_collection)

    if "sql_loaded" not in st.session_state:
        st.session_state.rag.load_sql_files()
        st.session_state.sql_loaded = True

    if "chain" not in st.session_state:
        st.session_state.chain = Chain()

    if "messages" not in st.session_state:
        st.session_state.messages = []
    
    if "chat_history" not in st.session_state:
        st.session_state.chat_history = []

    create_streamlit_app()