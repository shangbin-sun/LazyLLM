import json
import os
import requests
from typing import Tuple, List, Dict, Union, Any
import time
import lazyllm
from ..module import ModuleBase, Pipeline

class OnlineChatModuleBase(ModuleBase):
    """Reasoning interfaces using URLs"""

    def __init__(self,
                 model_type: str,
                 api_key: str,
                 base_url: str,
                 model_name: str,
                 system_prompt: str,
                 stream: bool,
                 trainable_models: List[str],
                 return_trace: bool = False):
        super().__init__(return_trace=return_trace)
        self._model_type = model_type
        if not api_key:
            raise ValueError("api_key is required")
        self._api_key = api_key
        self._base_url = base_url
        self._model_name = model_name
        self.system_prompt(prompt=system_prompt)
        self._stream = stream
        self.trainable_mobels = trainable_models
        self._set_headers()
        self._set_chat_url()

    def system_prompt(self, prompt: str = ""):
        """
        Set the system prompt of the openai interface
        """
        if len(prompt) > 0:
            self._system_prompt = {"role": "system", "content": prompt}
        else:
            self._system_prompt = {"role": "system", "content": "You are a helpful assistant."}

    def _set_headers(self):
        """Set the headers ans the chat url of the interface"""
        self._headers = {
            'Content-Type': 'application/json',
            'Authorization': 'Bearer ' + self._api_key
        }

    def _set_chat_url(self):
        """Set the chat url of the interface"""
        self._url = os.path.join(self._base_url, 'chat/completions')

    def _get_models_list(self):
        """Get the list of models"""
        url = os.path.join(self._base_url, 'models')
        headers = {'Authorization': 'Bearer ' + self._api_key}
        with requests.get(url, headers=headers) as r:
            if r.status_code != 200:
                raise requests.RequestException('\n'.join([c.decode('utf-8') for c in r.iter_content(None)]))

            res_json = r.json()
            return res_json

    def _parse_response_stream(self, response: str) -> str:
        """Parse the response from the interface"""
        chunk = response.decode('utf-8')[6:]
        return chunk

    def _parse_response_non_stream(self, response: str) -> Dict[str, Any]:
        """Parse the response from the interface"""
        cur_msg = json.loads(response)["choices"][0]["message"]
        return cur_msg

    def forward(self, __input: Union[Dict, str] = None, llm_chat_history: List[List[str]] = None, **kw):
        """LLM inference interface"""
        input = __input["query"] if isinstance(__input, dict) else __input

        content = [self._system_prompt]
        if llm_chat_history is not None:
            for item in llm_chat_history:
                if len(item) == 2:
                    content.append({"role": "user", "content": item[0]})
                    content.append({"role": "assistant", "content": item[1]})
                elif len(item) == 1:
                    content.append({"role": "user", "content": item[0]})
                    content.append({"role": "assistant", "content": ""})
                elif len(item) == 0:
                    continue
                else:
                    raise ValueError("llm_chat_history item length cannot be greater than 2")

        content.append({"role": "user", "content": input})
        data = {"messages": content}
        data["model"] = self._model_name
        data["stream"] = self._stream
        if len(kw) > 0:
            data.update(kw)

        def _impl_stream():
            """process http stream request"""
            with requests.post(self._url, json=data, headers=self._headers, stream=True) as r:
                if r.status_code != 200:  # request error
                    raise requests.RequestException('\n'.join([c.decode('utf-8') for c in r.iter_content(None)]))

                for line in r.iter_lines():
                    if len(line) == 0:
                        continue
                    chunk = self._parse_response_stream(line)
                    if chunk.startswith("{"):
                        msg = json.loads(chunk)["choices"][0]["delta"]
                        if len(msg) > 0:
                            yield msg
                        elif json.loads(chunk)["choices"][0]["finish_reason"] == "stop":
                            return ""
                        else:
                            # Not the expected data, throw an exception
                            raise ValueError(f"Unexpected msg: {msg}")
                    elif chunk == "[DONE]":
                        return ""
                    else:
                        # Not the expected chunk, throw an exception
                        raise ValueError(f"Unexpected chunk: {chunk}")

        def _impl_non_stream():
            """process http non-stream request"""
            with requests.post(self._url, json=data, headers=self._headers, stream=False) as r:
                if r.status_code != 200:  # request error
                    raise requests.RequestException(r.text)

                return self._parse_response_non_stream(r.text)

        if self._stream:
            return _impl_stream()
        else:
            return _impl_non_stream()

    def _set_template(self, template_message=None, input_key_name=None, template_headers=None):
        assert input_key_name is None or input_key_name in template_message.keys()
        self.template_message = template_message
        self.input_key_name = input_key_name
        self.template_headers = template_headers

    def _upload_train_file(self, train_file) -> str:
        """
        Upload train file to server.
        """
        raise NotImplementedError(f"{self._model_type} not implemented _upload_train_file method in subclass")

    def _create_finetuning_job(self, train_model, train_file_id, **kw) -> Tuple[str, str]:
        """
        Create fine-tuning job.
        """
        raise NotImplementedError(f"{self._model_type} not implemented _create_finetuning_job method in subclass")

    def _query_finetuning_job(self, fine_tuning_job_id) -> Tuple[str, str]:
        """
        Query fine-tuning job.
        """
        raise NotImplementedError(f"{self._model_type} not implemented _query_finetuning_job method in subclass")

    def set_train_tasks(self, train_file, **kw):
        """
        set train tasks
        """
        self._train_file = train_file
        self._train_parameters = kw

    def _get_train_tasks(self):
        """
        process train tasks
        """
        if not self._model_name or not self._train_file:
            raise ValueError("train_model and train_file is required")
        if self._model_name not in self.trainable_mobels:
            raise ValueError(f"{self._model_name} is not trainable")

        def _create_for_finetuning_job():
            """
            create for finetuning job to finish
            """
            file_id = self._upload_train_file(train_file=self._train_file)
            lazyllm.Log.info(f"{os.path.basename(self._train_file)} upload success! file id is {file_id}")
            (fine_tuning_job_id, status) = self._create_finetuning_job(self._model_name,
                                                                       file_id,
                                                                       **self._train_parameters)
            lazyllm.Log.info(f"fine tuning job {fine_tuning_job_id} created, status: {status}")

            if status.lower() == "failed":
                raise ValueError(f"Fine tuning job {fine_tuning_job_id} failed")
            import random
            while status.lower() != "succeeded":
                try:
                    # wait 10 seconds before querying again
                    time.sleep(random.randint(60, 120))
                    (fine_tuned_model, status) = self._query_finetuning_job(fine_tuning_job_id)
                    lazyllm.Log.info(f"fine tuning job {fine_tuning_job_id} status: {status}")
                    if status.lower() == "failed":
                        raise ValueError(f"Finetuning job {fine_tuning_job_id} failed")
                except ValueError:
                    raise ValueError(f"Finetuning job {fine_tuning_job_id} failed")

            lazyllm.Log.info(f"fine tuned model: {fine_tuned_model} finished")
            self._model_name = fine_tuned_model

        return Pipeline(_create_for_finetuning_job())

    def _create_deployment(self) -> Tuple[str, str]:
        """
        Create deployment.
        """
        raise NotImplementedError(f"{self._model_type} not implemented _create_deployment method in subclass")

    def _query_deployment(self, deployment_id) -> str:
        """
        Query deployment.
        """
        raise NotImplementedError(f"{self._model_type} not implemented _query_deployment method in subclass")

    def _get_deploy_tasks(self):
        """process deploy tasks"""
        def _start_for_deployment():
            """
            wait for deployment to finish
            """
            (deployment_id, status) = self._create_deployment()
            lazyllm.Log.info(f"deployment {deployment_id} created, status: {status}")

            if status.lower() == "failed":
                raise ValueError(f"Deployment task {deployment_id} failed")
            status = self._query_deployment(deployment_id)
            while status.lower() != "running":
                # wait 10 seconds before querying again
                time.sleep(10)
                status = self._query_deployment(deployment_id)
                lazyllm.Log.info(f"deployment {deployment_id} status: {status}")
                if status.lower() == "failed":
                    raise ValueError(f"Deployment task {deployment_id} failed")
            lazyllm.Log.info(f"deployment {deployment_id} finished")
        return Pipeline(_start_for_deployment())

    def __repr__(self):
        return lazyllm.make_repr('Module', 'OnlineChat', name=self._module_name, url=self._base_url,
                                 stream=self._stream, return_trace=self._return_trace)
