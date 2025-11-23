# CRB (Customer Revenue Bridge) SQL RAG Assistant

An intelligent SQL assistant powered by Streamlit, ChromaDB, and LLMs.  
It can:

- Explain SQL blocks and templates  
- Generate updated SQL scripts based on user requests  
- Retrieve the correct template using vector search  
- Maintain chat history and context  

## 🚀 Features
- **RAG-powered SQL retrieval** using ChromaDB (persistent storage)
- **Two modes**: Explanation & Script Generation
- **Fast embeddings** with BGE-small-en v1.5
- **LLM-based reasoning and SQL rewriting**
- **Streamlit UI with session persistence**
- **Clean separation of UI & RAG engine**

## 🛠 Tech Stack
- **Python 3.10+**
- **Streamlit**
- **ChromaDB (PersistentClient)**
- **Sentence Transformers**
- **Llama 3**
- **LangChain (optional)**


