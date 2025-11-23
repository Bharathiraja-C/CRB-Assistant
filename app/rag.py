import os
import uuid
import yaml
from utils import split_sql_ctes

class RAG:  

    def __init__(self, collection, metadata_file="app/resource/meta_data.yml"):

        self.collection = collection

        if os.path.exists(metadata_file):
            with open(metadata_file, "r") as f:
                self.metadata_map = yaml.safe_load(f)
        else:
            self.metadata_map = {}
            print("No metadata file found, continuing with empty metadata.")

    def load_sql_files(self,base_path="app/resource"):
        """Load SQL scripts and push them to ChromaDB"""
        for schema in os.listdir(base_path):
            schema_path = os.path.join(base_path, schema)
            if os.path.isdir(schema_path):
                for file_name in os.listdir(schema_path):
                    if file_name.endswith(".sql"):
                        script_name = file_name.replace(".sql", "")
                        file_path = os.path.join(schema_path, file_name)

                        with open(file_path, "r") as f:
                            sql_text = f.read()
                        
                        blocks = split_sql_ctes(sql_text)

                        for block, sql in blocks:

                            meta = self.metadata_map.get(block, {})

                            import json

                            def safe_meta(value):
                                """Convert None → '' and lists → JSON string"""
                                if value is None:
                                    return ""
                                if isinstance(value, list):
                                    return json.dumps(value)
                                return str(value)
                            
                            search_text = f"""
                                Block: {block}
                                Script: {script_name}
                                Schema: {schema}
                                Description: {meta.get("description", "")}
                                Inputs: {meta.get("inputs", [])}
                                Outputs: {meta.get("outputs", [])}
                                Dependencies: {meta.get("dependencies", [])}
                                Dialect: {meta.get("dialect", "PostgreSQL")}

                                SQL:
                                {sql}
                            """

                            metadata = {
                                "script_name": script_name,
                                "schema": safe_meta(schema),
                                "block" : block,
                                "inputs": safe_meta(meta.get("inputs")),
                                "outputs": safe_meta(meta.get("outputs")),
                                "dependencies": safe_meta(meta.get("dependencies")),
                                "order": safe_meta(meta.get("order")),
                                "dialect": safe_meta(meta.get("dialect", "PostgreSQL")),
                                "description": safe_meta(meta.get("description", ""))
                            }

                            self.collection.upsert(
                                documents=[search_text],
                                metadatas=[metadata],
                                ids=[str(uuid.uuid4())]
                            )

        print("✅ All SQL files loaded into ChromaDB")
        print("Count:", self.collection.count())


    def query_sql(self, user_prompt, n_results=5):
        results = self.collection.query(query_texts=[user_prompt], n_results=n_results)
        if not results["documents"] or not results["documents"][0]:
            return []

        sql_docs = []
        for doc, meta in zip(results["documents"][0], results["metadatas"][0]):
            # sql_docs.append({"sql": doc, "meta": meta})
            sql_docs.append({"meta": meta})

        return sql_docs
    

    def query_with_flow(self, user_prompt, n_results=5):
        # Step 1: Retrieve top-k relevant scripts/blocks
        results = self.collection.query(
            query_texts=[user_prompt],
            n_results=n_results,
            where={"schema": "analysis"}
        )

        candidates = []
        if results["documents"] and results["metadatas"]:
            for doc, meta in zip(results["documents"][0], results["metadatas"][0]):
                candidates.append({"sql": doc, "meta": meta})

        # Step 2: Expand with dependencies
        expanded = []
        seen = set()

        for c in candidates:
            script_name = c["meta"].get("script_name", "")
            if script_name not in seen:
                expanded.append(c)
                seen.add(script_name)

            # Dependencies
            dependencies = c["meta"].get("dependencies", [])
            if isinstance(dependencies, str):
                try:
                    import json
                    dependencies = json.loads(dependencies)  # in case it's a JSON string
                except Exception:
                    dependencies = [dependencies]

            for dep in dependencies:
                dep_results = self.collection.query(query_texts=[dep], n_results=3)
                if dep_results and dep_results["documents"]:
                    for d, m in zip(dep_results["documents"][0], dep_results["metadatas"][0]):
                        dep_script_name = m.get("script_name", "")
                        if dep_script_name not in seen:
                            expanded.append({"sql": d, "meta": m})
                            seen.add(dep_script_name)

        return expanded
