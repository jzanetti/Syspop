import numpy as np
import pandas as pd

"""
pip install llama-index
pip install llama-index-experimental
# pip install --upgrade --force-reinstall llama-index
pip install llama-index-embeddings-huggingface
pip install llama-index-llms-llama-cpp
pip install llama-index-embeddings-instructor
"""
"""
# Generate customer data
customer_data = {
    "Customer_ID": range(1, 51),
    "Name": ["Customer_" + str(i) for i in range(1, 51)],
    "City": ["City_" + str(np.random.randint(1, 11)) for _ in range(50)],
}
customers_df = pd.DataFrame(customer_data)

# Generate order data
order_data = {
    "Customer_ID": np.random.choice(range(1, 51), size=500),
    "Product_Name": np.random.choice(
        ["Product_A", "Product_B", "Product_C", "Product_D", "Product_E"], size=500
    ),
    "Order_Quantity": np.random.randint(1, 11, size=500),
    "Price_Per_Item": np.random.randint(
        10, 101, size=500
    ),  # Assuming price range from 10 to 100
}

# Calculate total price for each order
order_data["Total_Price"] = order_data["Order_Quantity"] * order_data["Price_Per_Item"]

orders_df = pd.DataFrame(order_data)

# Save dataframes to CSV files
customers_df.to_csv("customer_info.csv", index=False)
orders_df.to_csv("order_details.csv", index=False)
"""


from pickle import load as pickle_load

from llama_index.core import ServiceContext, set_global_service_context

# from llama_index.core.query_engine import PandasQueryEngine
from llama_index.embeddings.huggingface import HuggingFaceEmbedding
from llama_index.experimental.query_engine.pandas import PandasQueryEngine
from llama_index.llms.llama_cpp import LlamaCPP
from llama_index.llms.llama_cpp.llama_utils import (
    completion_to_prompt,
    messages_to_prompt,
)

df = pd.read_parquet("/tmp/syspop_test8/Auckland/syspop_base.parquet")

embed_model = HuggingFaceEmbedding(model_name="BAAI/bge-small-en-v1.5")

my_llm = LlamaCPP(
    model_path="/home/zhangs/Github/llm-abm/codellama-7b-instruct.Q8_0.gguf.2",  # llama-2-7b-chat.ggmlv3.q8_0.gguf, codellama-7b-instruct.Q8_0.gguf.2
    temperature=0.1,
    max_new_tokens=1024,
    context_window=5000,
    generate_kwargs={},
    model_kwargs={"n_gpu_layers": 30, "repetition_penalty": 1.5},
    messages_to_prompt=messages_to_prompt,
    completion_to_prompt=completion_to_prompt,
    verbose=False,
)

service_context = ServiceContext.from_defaults(
    llm=my_llm,
    chunk_size=1024,
    embed_model=embed_model,
)
set_global_service_context(service_context)

query_engine = PandasQueryEngine(df=df, verbose=False)
msg = "Can you summarize the ethnicity composition ? "
response = query_engine.query(msg)
print(response)
