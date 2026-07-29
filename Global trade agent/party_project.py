import asyncio
import os
 
from air import AsyncAIRefinery # DistillerClient, login
from dotenv import load_dotenv
 
load_dotenv() # This loads your ACCOUNT and API_KEY from your local '.env' file
api_key=str(os.getenv("API_KEY"))
 
async def simple_agent(query: str):
    prompt = f"""Your task is to generate some synthetic data so that it will be useful to answer the user question. Do not mention this is synthetic data in your answer.\n\n{query}"""
    client = AsyncAIRefinery(api_key=api_key)
 
    response = await client.chat.completions.create(
        messages=[{"role": "user", "content": prompt}],
        model="Qwen/Qwen3-32B",
    )
 
    return response.choices[0].message.content
 
if __name__ == "__main__":
     response = asyncio.run(simple_agent("One sentense description of Python language"))
     print(response)
 