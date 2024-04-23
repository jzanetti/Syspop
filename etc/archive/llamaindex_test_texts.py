import re
import time
from typing import List, Optional

from llama_cpp import Llama, LogitsProcessorList
from lmformatenforcer import CharacterLevelParser, JsonSchemaParser
from lmformatenforcer.integrations.llamacpp import build_llamacpp_logits_processor
from pydantic import BaseModel


class AnswerFormat(BaseModel):
    conversation_content: str
    conversation_mood: str
    # if_apple_eaten: bool


DEFAULT_SYSTEM_PROMPT = """\
You are a helpful, respectful and honest assistant. Always answer as helpfully as possible, while being safe.  Your answers should not include any harmful, unethical, racist, sexist, toxic, dangerous, or illegal content. Please ensure that your responses are socially unbiased and positive in nature.\n\nIf a question does not make any sense, or is not factually coherent, explain why instead of answering something not correct. If you don't know the answer to a question, please don't share false information.\
"""

print("Loading LLM")

LLM = Llama(
    model_path="/home/zhangs/Github/llm-abm/llama-2-7b-chat.ggmlv3.q8_0.gguf",
    verbose=False,
)


def get_prompt(message: str, system_prompt: str = DEFAULT_SYSTEM_PROMPT) -> str:
    return f"<s>[INST] <<SYS>>\n{system_prompt}\n<</SYS>>\n\n{message} [/INST]"


def llamacpp_with_character_level_parser(
    llm: Llama, prompt: str, character_level_parser: Optional[CharacterLevelParser]
) -> str:
    logits_processors: Optional[LogitsProcessorList] = None
    if character_level_parser:
        logits_processors = LogitsProcessorList(
            [build_llamacpp_logits_processor(llm, character_level_parser)]
        )

    output = llm(prompt, logits_processor=logits_processors, max_tokens=100)
    text: str = output["choices"][0]["text"]
    return text


def summarize_texts(question: str, answer: str):
    """Helper function to call ChatGPT API"""

    question_fmt = (
        f"The question is {question}. "
        + f"From the dataset, I got the answer as {answer}. "
        + f"Please convert the above into a declarative sentence/paragraph, and make it more illustrative"
    )

    question_with_schema = f"{question_fmt}{AnswerFormat.schema_json()}"
    prompt = get_prompt(question_with_schema)

    retries = 0
    while True:
        try:
            result = llamacpp_with_character_level_parser(
                LLM, prompt, JsonSchemaParser(AnswerFormat.schema())
            )

            if "false" in result:
                result = result.replace("false", "False")
            if "true" in result:
                result = result.replace("true", "True")

            result = eval(result)

            return {
                "answer": result["conversation_content"],
                "mood": result["conversation_mood"],
            }
        except:
            if retries == 3:
                raise Exception("Not able to get answers ...")
            retries += 1


question = "What is the number of people in City A ?"
answer = "1300"

question = "Can you summarize the ethnicity composition ?"
answer = "ethnicity\nEuropean    767048\nAsian       420655\nPacific     221893\nMaori       160983\nMELAA        32442\nName: count, dtype: int64"

proc_response = summarize_texts(question, answer)


"""
# create a text prompt
prompt = "Q: What are the names of the days of the week? A:"

# prompt = "What's the most "
# generate a response (takes several seconds)
prompt = "Q: What is the most common name in China? A:"
output = LLM(prompt)
output = LLM(prompt)
# display the response
print(output["choices"][0]["text"])
"""
