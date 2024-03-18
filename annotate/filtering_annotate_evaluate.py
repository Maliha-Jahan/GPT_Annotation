import os
import re
import sys
import time
import openai
import argparse
import pandas as pd
import numpy as np
import tiktoken
from copy import deepcopy


def get_limit(model_name):
    if model_name == "gpt-3.5-turbo":
        return 4096, 4060
    elif model_name == "gpt-3.5-turbo-16k":
        return 16384, 16350
    elif model_name == "gpt-4":
        return 8192, 8160
    elif model_name == "gpt-4-32k":
        return 32768, 32730


def get_reply(model_name, instruction, temp):
    org = "" # your key
    api_key = "" # your key
    openai.organization = org
    openai.api_key = api_key

    response = openai.ChatCompletion.create(
      model=model_name,
      messages=[
            {"role": "system", "content": "You are a helpful assistant."},
            {"role": "user", "content": instruction}
        ],
      temperature=temp,
      n=1
    )
    reply = response['choices'][0]
    reply_finish_reason = reply['finish_reason']
    reply_message = reply['message']['content']
    tokens = response['usage']['total_tokens']

    return reply_message, reply_finish_reason, tokens


def main(temperature, instruction, dir_path, save_path, relevant_files="", model_name="gpt-4"):
    os.makedirs(os.path.dirname(save_path), exist_ok=True)
    with open(instruction, "r") as fr:
        lines_ins = fr.readlines()

    encoding = tiktoken.encoding_for_model(model_name)

    files = os.listdir(dir_path[0])
    files.sort()

    gt_files = os.listdir(relevant_files)

    df_file_spk = {
        "File": [],
        "Speakers": [],
    }

    fps = 0
    fns = 0
    total = len(files)
    with open(save_path, 'w') as fw:
        for idx, file_ in enumerate(files):
            df_file_spk["File"].append(file_)
            with open(os.path.join(dir_path, file_), "r") as fr:
                lines_prompt = fr.readlines()

            lines = lines_ins + ["\n"] + lines_prompt + ["\n<End of transcript>"]
            query = "".join(lines)

            encoded_query = encoding.encode(query)
            print(file_, len(encoded_query))

            token_limit, limit = get_limit(model_name)
            if len(encoded_query) > limit:
                instruction = "".join(lines_ins + ["\n"])
                encoded_instruction = encoding.encode(instruction)
                n_ins = len(encoded_instruction)
                limit -= n_ins

                prompt = "".join(lines_prompt)
                encoded_prompt = encoding.encode(prompt)
                parts = []

                p = 0
                while len(encoded_prompt) > limit:
                    chunk = encoding.decode(encoded_prompt[:limit])
                    p = chunk.rfind('SPEAKER_')
                    if p < 0:
                        # print("P IS LESS THAN 0!!!!!!!!!!!!!!!")
                        encoded_prompt = encoded_prompt[limit:]
                        continue
                    elif p == 0:
                        # print("P IS ZERO!")
                        p = len(chunk)
                    parts.append(instruction + chunk[:p] + "\n<End of transcript>")
                    encoded_prompt = encoding.encode(chunk[p:] + encoding.decode(encoded_prompt[limit:]))
                else:
                    chunk = encoding.decode(encoded_prompt)
                    parts.append(instruction + chunk)

            else:
                parts = [query]

            relevance_count = 0
            total_success = 0
            for ip, part in enumerate(parts):
                # time.sleep(30)
                flag = True
                try_count = 0
                while flag:
                    try:
                        reply_message, reply_finish_reason, tokens = get_reply(model_name, part, temperature)
                        flag = False
                        total_success += 1
                        if reply_finish_reason != 'stop':
                            if tokens > token_limit:
                                print("Stopped due to token number exceeding limit")
                            else:
                                print("Stopped unexpectedly")
                    except Exception as e:
                        print(file_, "exception", e)
                        if "overloaded with other requests" in str(e):
                            flag = True
                        elif ("6ms" in str(e)) or ("exception Rate limit reached" in str(e)):
                            time.sleep(60)
                            flag = True
                            try_count += 1

                        if try_count > 10:
                            print("\nFAILED AFTER RETRYING. FILE: {}, PART: {}".format(file_, ip))
                            break
                        # continue
                if flag:
                    continue
                if "yes" in reply_message.lower():
                    relevance_count += 1
                elif "no" not in reply_message.lower():
                    print("REPLY UNCLEAR. COUNTING AS IRRELEVANT FILE. THE REPLY: {}".format(reply_message))

            if total_success == 0:
                print("\nFAILED ALL PARTS AFTER RETRYING. FILE: {}".format(file_, ip))
                total -= 1
                continue
            relevance_count = relevance_count/len(parts)

            if relevance_count >= 0.5:
                message = f"RELEVANT, {relevance_count}"
                if (relevant_files != "") and (file_ not in gt_files):
                    fps += 1
            else:
                message = f"IRRELEVANT, {relevance_count}"
                if (relevant_files != "") and (file_ in gt_files):
                    fns += 1

            message = "{} {}\n".format(file_, message)

            print(message)
            fw.write(message)

        if (relevant_files != ""):
            message = "result: FP: {} out of {}\nresult: FN: {} out of {}\n".format(fps, total, fns, total)
            print(message)
            fw.write(message)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-t',
                        '--temperature',
                        help="temperature parameter for chat gpt",
                        type=float,
                        required=True)
    parser.add_argument('-i',
                        '--instruction',
                        help="path to text instruction for chat gpt",
                        type=str,
                        required=True)
    parser.add_argument('-d',
                        '--dir-path',
                        help="path to the directory of transcript files",
                        type=str,
                        required=True)
    parser.add_argument('-f',
                        '--relevant-files',
                        help="ground truth or relevant files. the files that are not in this directory/list "
                             "will be ignored. The string can only be either a string containing the names of the relevant "
                             "files separated by spaces or the path to a directory containing only the relevant files",
                        type=str,
                        default="",
                        required=False)
    parser.add_argument('-s',
                        '--save-path',
                        help="path to save the processed csv gpt annotation",
                        type=str,
                        required=True)
    parser.add_argument('-m',
                        '--model-name',
                        help="chat gpt model name",
                        type=str,
                        default="gpt-4",
                        required=False)

    args = parser.parse_args()
    main(**vars(args))
