import numpy as np

from openai import OpenAI

from src.vector_storage import get_similar_gdpr_documents, get_similar_ai_act_documents
from src.rag_pipeline.configs import EMBEDDINGS_MODEL

client = OpenAI()


def get_completion_from_messages(messages, model="gpt-4o-mini", temperature=0, max_tokens=1000):
    response = client.chat.completions.create(
        model=model,
        messages=messages,
        temperature=temperature,
        max_tokens=max_tokens,
    )

    return response.choices[0].message.content


def get_query_embedding_array(query: str) -> np.array:
    response = client.embeddings.create(
        input=[query.replace("\n", " ")],
        model=EMBEDDINGS_MODEL
    )

    return np.array(response.data[0].embedding)


def process_input_with_retrieval(user_input: str):
    delimiter = "```"

    # Step 1: Get documents related to the user input from database
    related_docs = (
        get_similar_gdpr_documents(
            embedding_array=get_query_embedding_array(user_input)
        ) +
        get_similar_ai_act_documents(
            embedding_array=get_query_embedding_array(user_input)
        )
    )

    # Step 2: Get completion from OpenAI API
    # Set system message to help set appropriate tone and context for model
    system_message = f"""
    You are a friendly chatbot. \
    You can answer questions about The European Data Protection Regulation (GDPR) and EU Artificial Intelligence Act (AI Act). \
    You respond in a concise, technically credible tone. \
    """

    # Prepare messages to pass to model
    # Delimiter is used to help the model understand the where the user_input starts and ends
    messages = [
        {
            "role": "system",
            "content": system_message
        },
        {
            "role": "user",
            "content": f"{delimiter}{user_input}{delimiter}"
        },
        {
            "role": "assistant",
            "content":
                f"Relevant GDPR and AI Act articles: "
                f"\n {related_docs[0][0]} \n {related_docs[1][0]} \n {related_docs[2][0]}"
                f"\n {related_docs[3][0]} \n {related_docs[4][0]} \n {related_docs[5][0]}"
        }
    ]

    return get_completion_from_messages(messages)
