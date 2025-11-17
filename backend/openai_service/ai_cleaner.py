from openai import OpenAI, RateLimitError, APIError, APIConnectionError, AuthenticationError
from fastapi import HTTPException
import os
from dotenv import load_dotenv


load_dotenv()  

client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))


# SYSTEM_PROMPT = """You are a data cleaner. Your job is to normalize messy user input into a clean structure.

# Inputs:
# - Exclude keywords: optional, clean into a list (if provided)

# Always return a JSON object like:
# {
#   "exclude_keywords": [...]
# }
# """

# def clean_input_with_ai(exclude_keywords: str = ""):
#     user_prompt = f"""
# Exclude Keywords (optional):
# {exclude_keywords}
# """
#     try:
#         response = client.chat.completions.create(
#             model="gpt-3.5-turbo",
#             messages=[
#                 {"role": "system", "content": SYSTEM_PROMPT},
#                 {"role": "user", "content": user_prompt}
#             ],
#             temperature=0.3
#         )
#         content = response.choices[0].message.content
#         return content

#     except RateLimitError:
#         raise HTTPException(
#             status_code=429,
#             detail="You've reached your OpenAI usage limit. Please check your quota or try again later."
#         )

#     except AuthenticationError:
#         raise HTTPException(
#             status_code=401,
#             detail="OpenAI authentication failed. Please verify your API key."
#         )

#     except APIConnectionError:
#         raise HTTPException(
#             status_code=502,
#             detail="Failed to connect to OpenAI servers. Please try again in a few moments."
#         )

#     except APIError:
#         raise HTTPException(
#             status_code=500,
#             detail="An internal error occurred with OpenAI. Please try again later."
#         )

#     except Exception as e:
#         raise HTTPException(
#             status_code=500,
#             detail=f"An unexpected error occurred: {str(e)}"
#         )
    

def translate_title(title: str):
    """
    Detects if the title is in a foreign language.
    If yes → returns translated English title.
    If already English → returns False.
    """
    prompt = f"""
    Detect the language of this job title: "{title}".
    If it is English, reply with "ENGLISH_ONLY".
    If it is not English, translate it to English and return only the translated title.
    """

    response = client.chat.completions.create(
        model="gpt-4o-mini",  # or gpt-4o / gpt-5 depending on tier
        messages=[{"role": "user", "content": prompt}],
        temperature=0
    )

    result = response.choices[0].message.content.strip()

    if result == "ENGLISH_ONLY":
        return False
    
    return result
