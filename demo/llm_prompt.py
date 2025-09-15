from openai import OpenAI

class LLMPrompt:

    def __init__(self, model_path):        
        if "gpt" in model_path:     
            self.gpt_client = OpenAI(api_key="", base_url="") # TODO: set your key and base_url
              
        elif "kimi" in model_path:
            self.kimi_client = OpenAI(api_key="", base_url="") # TODO: set your key and base_url
       
        self.model_path = model_path
   
    def ask_LLM(self, ask_prompt, system_role):
        if "gpt-4o" in self.model_path:
            chat_completion = self.gpt_client.chat.completions.create(
                model="gpt-4o-lastest",
                messages=[
                    {"role": "system", "content": f"{system_role}"},
                    {"role": "user", "content": f"{ask_prompt}"},
                ],
            )
            return_info = chat_completion.choices[0].message.content
            return return_info



        elif "kimi" in self.model_path:

            chat_completion = self.kimi_client.chat.completions.create(
                model="Moonshot-Kimi-K2-Instruct",
                messages=[
                    {"role": "system", "content": f"{system_role}"},
                    {"role": "user", "content": f"{ask_prompt}"},
                ],
            )
            return_info = chat_completion.choices[0].message.content
            return return_info

