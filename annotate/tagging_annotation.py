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


valid_types = ["self-identification", "other-person in-interaction identification",
               "other-person outside-interaction identification", "unknown"]
valid_ethnicities = ["hispanic or latinx", "non-hispanic or non-latinx", "unknown"]
valid_races = ["white", "black or african american", "asian", "american indian or alaska native",
               "native hawaiian or other pacific islander", "multiracial", "unknown"]


def get_limit(model_name):
    if model_name == "gpt-3.5-turbo":
        return 4096, 2730
    elif model_name == "gpt-3.5-turbo-16k":
        return 16384, 11000
    elif model_name == "gpt-4":
        return 8192, 5500
    elif model_name == "gpt-4-32k":
        return 32768, 22000


def get_reply(model_name, instruction, temp):
    org = "" # your organization key
    api_key = "" # your api key
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


def get_results(reply_message, file_):
    results = [item for item in reply_message.split("Identifier Speaker") if item.strip() != ""]
    df = {"File": [], "Source": [], "Target": [], "Ethnicity": [], "National Origin": [], "Race": [], "Type": [], "Line": []}

    for spk in results:
        if ("SPEAKER_" not in spk) and ("speaker_" not in spk):
            continue
        df["File"].append(file_)
        spk_lower = spk.lower()

        p = spk_lower.find("speaker_")
        p_e = spk_lower.find("identified speaker")
        df["Source"].append(spk[p: p_e].replace(",", "").replace(":", "").strip().lower())
        spk = spk[p_e:]

        substr = "Identified Speaker"
        p = spk_lower.find(substr.lower()) + len(substr)
        p_e = spk_lower.find("line")
        df["Target"].append(spk[p: p_e].replace(",", "").replace(":", "").strip().lower())
        spk = spk[p_e:]

        substr = "Line"
        p = 0 + len(substr)
        p_e = spk_lower.find("speaker ethnicity")
        df["Line"].append(spk[p: p_e].replace(",", "").replace(":", "").strip().lower())
        spk = spk[p_e:]

        substr = "Speaker Ethnicity"
        p = spk_lower.find(substr.lower()) + len(substr)
        p_e = spk_lower.find("speaker national origin")
        df["Ethnicity"].append(spk[p: p_e].replace(",", "").replace(":", "").strip().lower())
        spk = spk[p_e:]

        substr = "Speaker National Origin"
        p = 0 + len(substr)
        p_e = spk_lower.find("speaker race")
        df["National Origin"].append(spk[p: p_e].replace(",", "").replace(":", "").strip().lower())
        spk = spk[p_e:]

        substr = "Speaker Race"
        p = 0 + len(substr)
        p_e = spk_lower.find("category")
        df["Race"].append(spk[p: p_e].replace(",", "").replace(":", "").strip().lower())
        spk = spk[p_e:]

        substr = "Category"
        p = 0 + len(substr)
        p_e = spk.find("\n")
        if p_e > 0:
            df["Type"].append(spk[p: p_e].replace(",", "").replace(":", "").strip().lower())
        else:
            df["Type"].append(spk[p:].replace(",", "").replace(":", "").strip().lower())

    return df


def modify_results(df):
    df_modified = df
    invalid = ["", "n/a", "na", "n\\a", "none", " ", "-", "not mentioned", "not applicable"
               "not found", "not identified", "unknown", "not specified"]

    for i in range(len(df_modified["Source"])):
        if (df_modified["File"][i] in invalid)\
                or (df_modified["Source"][i] in invalid)\
                or (df_modified["Type"][i] in invalid) or (df_modified["Line"][i] in invalid)\
                or ((df_modified["Ethnicity"][i] in invalid)
                    and (df_modified["National Origin"][i] in invalid)
                    and (df_modified["Race"][i] in invalid)):
            np.delete(df_modified["File"], i)
            np.delete(df_modified["Source"], i)
            np.delete(df_modified["Target"], i)
            np.delete(df_modified["Ethnicity"], i)
            np.delete(df_modified["National Origin"], i)
            np.delete(df_modified["Race"], i)
            np.delete(df_modified["Type"], i)
            np.delete(df_modified["Line"], i)
    return df_modified


def get_speakers(lines):
    prompt = "".join(lines)
    speakers = prompt.split("SPEAKER_")
    speakers = [("SPEAKER_" + l[:2]).lower() for l in speakers if l[:2].isnumeric()]
    speakers = list(set(speakers))
    return speakers


def split_multiples(df, col, valid_vals):
    df_extended = []
    for i in range(df.shape[0]):
        temp = str(df[i, col])
        temp = re.sub("([\(\[]).*?([\)\]])", "", temp)
        temp = str(temp).replace("/", ",").replace("&", ",")
        temps = temp.split(",")
        for t, temp in enumerate(temps):
            temp = re.sub('[^a-zA-Z\s]+', " ", temp)
            _RE_COMBINE_WHITESPACE = re.compile(r"\s+")
            temp = _RE_COMBINE_WHITESPACE.sub(" ", temp).strip()

            if valid_vals is None:
                if t == 0:
                    df[i, col] = temp
                else:
                    row = deepcopy(df[i, :])
                    row[col] = temp
                    df_extended.append(row.tolist())
            else:
                flag = False
                for j in range(len(valid_vals)):
                    if (temp in valid_vals[j]) or (valid_vals[j] in temp):
                        if t == 0:
                            df[i, col] = valid_vals[j]
                        else:
                            row = deepcopy(df[i, :])
                            row[col] = valid_vals[j]
                            df_extended.append(row.tolist())
                        flag = True
                        break
                if not flag:
                    print("INVALID RACE: ", df[i, col])
                    if t == 0:
                        df[i, col] = temp
                    else:
                        row = deepcopy(df[i, :])
                        row[col] = temp
                        df_extended.append(row.tolist())

    if df_extended != []:
        df = np.concatenate((df, np.array(df_extended)), axis=0)

    return df


def post_process(df):
    df = df.values
    df[np.where((df == "unknown") | (df == "Unknown") | (df == "n/a") | (df == "na")
                | (df == "N/A") | (df == "NA"))] = "unknown"
    file, source, target, ethnicity, national_origin, race, type, line = np.arange(df.shape[-1]).tolist()
    idx = (df[:, source] != "unknown") & (df[:, line] != "unknown") \
          & (df[:, line] != "") & ((df[:, national_origin] != "unknown")
                                           | (df[:, race] != "unknown"))

    df = df[idx, :]
    df_del = []

    for i in range(df.shape[0]):
        temp = str(df[i, type]).lower()
        temp = re.sub("([\(\[]).*?([\)\]])", "", temp)
        if (temp in valid_types[0].lower()) or (valid_types[0].lower() in temp):
            df[i, type] = valid_types[0]
        elif (temp in valid_types[1].lower()) or (valid_types[1].lower() in temp):
            df[i, type] = valid_types[1]
        elif (temp in valid_types[2].lower()) or (valid_types[2].lower() in temp):
            df[i, type] = valid_types[2] 
        else:
            df[i, type] = "unknown"
            df_del.append(i)
    df = np.delete(df, (df_del), axis=0)

    idx2 = np.where(df[:, type] == "self-identification")
    df[idx2, target] = df[idx2, source]

    df = split_multiples(df, race, valid_races)
    df = split_multiples(df, national_origin, None)
    return df


def main(temperature, instruction, dir_path, save_path, selected_files="", model_name="gpt-4"):
    os.makedirs(os.path.dirname(save_path), exist_ok=True)
    with open(instruction, "r") as fr:
        lines_ins = fr.readlines()

    encoding = tiktoken.encoding_for_model(model_name)
    df = {"File": [], "Source": [], "Target": [], "Ethnicity": [],
          "National Origin": [], "Race": [], "Type": [], "Line": []}

    files = os.listdir(dir_path)
    files.sort()

    if selected_files != "":
        if "/" in selected_files:
            do_files = [d.replace(d.split(".")[-1], "txt") for d in os.listdir(selected_files)]
        else:
            do_files = selected_files.split()

    df_file_spk = {
        "File": [],
        "Speakers": [],
    }

    count=0
    for idx, file_ in enumerate(files):
        if selected_files != "":
            if file_ not in do_files:
                continue

        df_file_spk["File"].append(file_)
        try:
            with open(os.path.join(dir_path, file_), "r") as fr:
                lines_prompt = fr.readlines()
        except:
            continue

        speakers = get_speakers(lines_prompt)
        df_file_spk["Speakers"].append(speakers)

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

        result_dict = {"File": [], "Source": [], "Target": [], "Ethnicity": [],
                       "National Origin": [], "Race": [], "Type": [], "Line": []}

        for ip, part in enumerate(parts):
            try:
                # print(ip, len(encoding.encode(part)))
                time.sleep(60)
                flag = True
                while flag:
                    try:
                        reply_message, reply_finish_reason, tokens = get_reply(model_name, part, temperature)
                        flag = False
                        if reply_finish_reason != 'stop':
                            if tokens > token_limit:
                                print("Stopped due to token number exceeding limit")
                            else:
                                print("Stopped unexpectedly")
                    except Exception as e:
                        print(file_, "exception", e)
                        if "overloaded with other requests" in str(e):
                            flag = True
                        elif ("6ms" in str(e)) or ("rate limit reached" in str(e).lower()):
                            time.sleep(65)
                            flag = True

                rd = get_results(reply_message, file_)

                result_dict["File"].extend(rd["File"])
                result_dict["Source"].extend(rd["Source"])
                result_dict["Target"].extend(rd["Target"])
                result_dict["Ethnicity"].extend(rd["Ethnicity"])
                result_dict["National Origin"].extend(rd["National Origin"])
                result_dict["Race"].extend(rd["Race"])
                result_dict["Type"].extend(rd["Type"])
                result_dict["Line"].extend(rd["Line"])
            except Exception as e:
                print("EXCEPTION:", e)

        result_dict["File"] = np.array(result_dict["File"])
        result_dict["Source"] = np.array(result_dict["Source"])
        result_dict["Target"] = np.array(result_dict["Target"])
        result_dict["Ethnicity"] = np.array(result_dict["Ethnicity"])
        result_dict["National Origin"] = np.array(result_dict["National Origin"])
        result_dict["Race"] = np.array(result_dict["Race"])
        result_dict["Type"] = np.array(result_dict["Type"])
        result_dict["Line"] = np.array(result_dict["Line"])

        df_modified = modify_results(result_dict)

        df["File"].extend(df_modified["File"])
        df["Source"].extend(df_modified["Source"])
        df["Target"].extend(df_modified["Target"])
        df["Ethnicity"].extend(df_modified["Ethnicity"])
        df["National Origin"].extend(df_modified["National Origin"])
        df["Race"].extend(df_modified["Race"])
        df["Type"].extend(df_modified["Type"])
        df["Line"].extend(df_modified["Line"])

        if (count+1) % 50 == 0 or (count == 10):
            df_temp = pd.DataFrame.from_dict(df)
            ext = save_path.split(".")[-1]
            df_temp.to_csv(save_path.replace("." + ext, f"_{(count + 1) // 50}." + ext), index=False,
                           header=["File", "Source", "Target", "Ethnicity", "National Origin", "Race", "Type", "Line"])
        count += 1

    df = pd.DataFrame.from_dict(df)

    df.to_csv(save_path, index=False,
              header=["File", "Source", "Target", "Ethnicity", "National Origin", "Race", "Type", "Line"])

    print("\n\nStarting Preprocessing")
    df = post_process(df)
    df = pd.DataFrame(df)
    print("\nFinished Preprocessing")
    df.to_csv(save_path.replace("."+save_path.split(".")[-1], "_trim."+save_path.split(".")[-1]),
              index=False, header=["File", "Source", "Target", "Ethnicity", "National Origin", "Race", "Type", "Line"])
    df_file_spk = pd.DataFrame.from_dict(df_file_spk)
    df_file_spk.to_csv(save_path.replace("."+save_path.split(".")[-1], "_files_speakers."+save_path.split(".")[-1]), index=False)


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
                        '--selected-files',
                        help="if only selected files should be processed. the files that are not in this directory/list "
                             "will be ignored. The string can only be either a string containing the names of the selected "
                             "files separated by spaces or the path to a directory containing only the selected files",
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

