import ollama as ol
global prompts, models, messages
roles = ['assistant', 'user', 'system']
prompts = {
    "prompt1" : "接下来请始终用一句话回复。同意对方的观点，请直接输出1，反之输入0。给出一句话作为理由。",
    "prompt2" : "衡量新闻的信息披露质量 用 事件 时间 地点 人物 数字 情感 是否合理",
}
models = {
    "llama3" : "llama3.1:8b",
    "finllama3" : "martain7r/finance-llama-8b:q4_k_m",
    "deepseek" : "deepseek-v3.1:671b-cloud",
}
messages = [
    {'role': roles[2], 'content': prompts['prompt1']},
    {'role': roles[1], 'content': prompts['prompt2']},
]
def model_conversation(name):
    response = ol.chat(model=models[name], messages=messages)
    reply = response['message']['content']
    print('Response is\n', response)
    print('Reply is\n', reply)

if __name__ == "__main__":
    # 启动对话
    print("Launching Model...")
    model_conversation('llama3')
    print("End.")