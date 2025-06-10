# main.py
import sys

from langchain_agent import answer_question

if __name__ == "__main__":
    # try:
    #     while True:
    #         try:
    user_question = input("Please enter your question: ")
    answer = answer_question(user_question)
    print("\n🗣️ Final Answer:", answer)
            # except Exception as e:
            #     print(f"Failed to generate answer. Error: {e}")
    # except KeyboardInterrupt:
    #     sys.exit(0)