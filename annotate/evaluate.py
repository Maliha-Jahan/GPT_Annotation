import copy
import os
import re
import sys
import json
from copy import deepcopy
from difflib import SequenceMatcher

import pandas
import pandas as pd
import numpy as np
import argparse


############# Change filename here. The file should be a json file mapping various ways of saying a 
# nationality to that nationality. I.E. America to American, USA to American, United States to American etc.
valid_national_origin_file = ""
with open(valid_national_origin_file) as f:
    valid_national_origins = json.load(f)

valid_types_for_indexing = ["self-identification", "other-person in-interaction identification",
               "other-person outside-interaction identification", "unknown"]
valid_ethnicities_for_indexing = ["hispanic or latinx", "non-hispanic or non-latinx", "unknown"]
valid_races_for_indexing = ["white", "black or african american", "asian", "american indian or alaska native",
               "native hawaiian or other pacific islander", "multiracial", "unknown"]
valid_types = ["self-identification", "other-person in-interaction identification",
               "other-person outside-interaction identification", "-1"]
# valid_types0 = ["self", "2nd person", "3rd person", "-1"]
valid_ethnicities = ["hispanic or latinx", "non-hispanic or non-latinx", "-1"]
valid_races = ["white", "black or african american", "asian", "american indian or alaska native",
               "native hawaiian or other pacific islander", "multiracial", "-1"]

valid_columns = ["id_fier", "id_fied", "ethnicity", "national_origin", "race", "type", "line"]
valid_attr = ["ethnicity", "national_origin", "race"]
col_idx = [1, 2, 3, 4, 5, 6, 7]


def split_nats(df, col, valid_vals):
    df_del, df_extended = [], []
    for i in range(df.shape[0]):
        temp = str(df[i, col]).lower()
        temp = re.sub("([\(\[]).*?([\)\]])", "", temp)
        temp = str(temp).replace("/", ",").replace("&", ",")

        if re.sub('[^a-zA-Z\s]+', " ", temp.lower()) in valid_vals.keys():
            df[i, col] = valid_vals[re.sub('[^a-zA-Z\s]+', " ", temp)]
        else:
            temp = str(temp).replace("and", ",").replace(" ", ",")
        temps = temp.split(",")
        for t, temp in enumerate(temps):
            temp = re.sub('[^a-zA-Z\s]+', "", temp)
            _RE_COMBINE_WHITESPACE = re.compile(r"\s+")
            temp = _RE_COMBINE_WHITESPACE.sub(" ", temp).strip()
            if temp in ["people", "american", "america", "new", "north", "south", "east", "west", "island", "islands", "islander", "republic", "of", "", " ", "or", "and"]:
                continue

            if valid_vals is None:
                if t == 0:
                    df[i, col] = temp
                else:
                    row = deepcopy(df[i, :])
                    row[col] = temp
                    df_extended.append(row.tolist())
                continue
            else:
                # print("INVALID0 National origin: ", df[i, col])
                if temp in valid_vals.keys():
                    if t == 0:
                        df[i, col] = valid_vals[temp]
                    else:
                        row = deepcopy(df[i, :])
                        row[col] = valid_vals[temp]
                        df_extended.append(row.tolist())
                else:
                    # print("INVALID1 National origin: ", df[i, col])
                    flag = False
                    for key in valid_vals.keys():
                        if (temp in key) or (key in temp):
                            if t == 0:
                                df[i, col] = valid_vals[key]
                            else:
                                row = deepcopy(df[i, :])
                                row[col] = valid_vals[key]
                                df_extended.append(row.tolist())
                            flag = True
                            break

                    if not flag:
                        # print("INVALID2 National origin: ", df[i, col])
                        # if t == 0:
                        #     df_del.append(i)
                        if t == 0:
                            df[i, col] = temp
                        else:
                            row = deepcopy(df[i, :])
                            row[col] = temp
                            df_extended.append(row.tolist())

    # df = np.delete(df, (df_del), axis=0)
    if df_extended != []:
        df = np.concatenate((df, np.array(df_extended)), axis=0)

    return df


def split_races(df, col, valid_vals):
    df_del, df_extended = [], []
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
                    # print("INVALID RACE: ", df[i, col])
                    # if t == 0:
                    #     df_del.append(i)
                    if t == 0:
                        df[i, col] = temp
                    else:
                        row = deepcopy(df[i, :])
                        row[col] = temp
                        df_extended.append(row.tolist())

    # df = np.delete(df, (df_del), axis=0)
    if df_extended != []:
        df = np.concatenate((df, np.array(df_extended)), axis=0)

    return df


def pre_process(df):
    # File,Source,Target,Ethnicity,National Origin,Race,Type,Line
    # df = df.values
    df[np.where((df == "unknown") | (df == "Unknown") | (df == "n/a") | (df == "na")
                | (df == "N/A") | (df == "NA"))] = "unknown"
    file, source, target, ethnicity, national_origin, race, type, line = np.arange(df.shape[-1]).tolist()
    idx = (df[:, source] != "unknown") & (df[:, line] != "unknown") \
          & (df[:, line] != "") & ((df[:, national_origin] != "unknown")
                                           | (df[:, race] != "unknown"))
    df = df[idx, :]
    df_del = []

    for i in range(df.shape[0]):
        temp = str(df[i, type])
        # temp = re.sub("([\(\[]).*?([\)\]])", "\g<1>\g<2>", temp)
        temp = re.sub("([\(\[]).*?([\)\]])", "", temp)
        if (temp in valid_types_for_indexing[0]) or (valid_types_for_indexing[0] in temp):
            df[i, type] = valid_types_for_indexing[0]
        elif (temp in valid_types_for_indexing[1]) or (valid_types_for_indexing[1] in temp):
            df[i, type] = valid_types_for_indexing[1]
        elif (temp in valid_types_for_indexing[2]) or (valid_types_for_indexing[2] in temp):
            df[i, type] = valid_types_for_indexing[2]
        else:
            print("type unknown", df[i, type])
            df[i, type] = "unknown"
            df_del.append(i)

    df = np.delete(df, (df_del), axis=0)

    df = split_races(df, race, valid_races_for_indexing)
    df = split_nats(df, national_origin, valid_national_origins)
    return df


def main(file_path_gpt, file_path_human, columns, preprocess=False):
    df_human = pd.read_csv(file_path_human, header=0, keep_default_na=False, index_col=False)
    df_human_np = df_human.values

    if preprocess:
        df_human_np = pre_process(df_human_np)
        df = pandas.DataFrame(df_human_np)
        df.to_csv(file_path_human.replace(".csv", "_new.csv"), index=False,
                  header=["File", "Source", "Target", "Ethnicity", "National Origin", "Race", "Type", "Line"])

    df_gpt = pd.read_csv(file_path_gpt, header=0, keep_default_na=False, index_col=False)
    df_gpt_np = df_gpt.values  # row x col

    if preprocess:
        df_gpt_np = pre_process(df_gpt_np)
        df = pandas.DataFrame(df_gpt_np)
        df.to_csv(file_path_gpt.replace(".csv", "_new.csv"), index=False,
                  header=["File", "Source", "Target", "Ethnicity", "National Origin", "Race", "Type", "Line"])

    df_gpt_np[np.where(df_gpt_np == "unknown")] = "-1"
    df_human_np[np.where(df_human_np == "unknown")] = "-1"

    cols = [col_idx[valid_columns.index(i)] for i in columns]
    cols.sort()
    df_gpt_np = df_gpt_np[:, [0] + cols]
    df_human_np = df_human_np[:, [0] + cols]

    columns = ["file"] + columns
    columns = {columns[i]: i for i in range(len(columns))}

    idx_gpt = np.array([False] * len(df_gpt_np)).astype(bool)
    idx_human = np.array([False] * len(df_human_np)).astype(bool)
    for i in valid_attr:
        if i in columns.keys():
            idx_gpt = idx_gpt | (df_gpt_np[:, columns[i]] != "-1")
            idx_human = idx_human | (df_human_np[:, columns[i]] != "-1")
    if "id_fier" in columns.keys():
        idx_gpt = idx_gpt & (df_gpt_np[:, columns["id_fier"]] != "-1")
        idx_human = idx_human & (df_human_np[:, columns["id_fier"]] != "-1")
    elif "id_fied" in columns.keys():
        idx_gpt = idx_gpt & (df_gpt_np[:, columns["id_fied"]] != "-1")
        idx_human = idx_human & (df_human_np[:, columns["id_fied"]] != "-1")

    df_gpt_np = df_gpt_np[idx_gpt, :]
    df_human_np = df_human_np[idx_human, :]

    if "national_origin" in columns.keys():
        nat_unique, nat_inv = np.unique(df_human_np[:, columns["national_origin"]], return_inverse=True)
        df_human_np[:, columns["national_origin"]] = nat_inv

    idx_gpt = []
    idx_human = []

    for i in range(df_gpt_np.shape[0]):
        df_gpt_np[i, columns["file"]] = int(df_gpt_np[i, columns["file"]].split(".")[0])
        if "id_fied" in columns.keys():
            try:
                temp_spk =  re.sub('[^0-9]+', "", df_gpt_np[i, columns["id_fied"]].lower())
                df_gpt_np[i, columns["id_fied"]] = int(temp_spk)
            except Exception as e:
                if temp_spk == "":
                    # print("exception spk gpt: empty")
                    idx_gpt.append(i)
                else:
                    # print("exception spk gpt", e, x)
                    df_gpt_np[i, columns["id_fied"]] = "-1"
                    if "id_fier" not in columns.keys():
                        idx_gpt.append(i)
                        continue
        if "id_fier" in columns.keys():
            try:
                df_gpt_np[i, columns["id_fier"]] = int(re.sub('[^0-9]+', "", df_gpt_np[i, columns["id_fier"]].lower()))
            except:
                idx_gpt.append(i)
                continue

        if "type" in columns.keys():
            df_gpt_np[i, columns["type"]] = valid_types.index(df_gpt_np[i, columns["type"]])

        if "race" in columns.keys():
            try:
                df_gpt_np[i, columns["race"]] = valid_races.index(df_gpt_np[i, columns["race"]].replace('alaskan', 'alaska'))
            except Exception as e:
                # print(e, df_gpt_np[i, columns["race"]])
                idx_gpt.append(i)
        if "national_origin" in columns.keys():
            nat = str(df_gpt_np[i, columns["national_origin"]])
            scores = np.array([SequenceMatcher(None, nat, u).ratio() for u in nat_unique])
            nat_idx = np.arange(len(scores))
            bools = np.array([(nat in u) or (u in nat) for u in nat_unique.tolist()]).astype(bool)
            if bools.astype(int).sum() > 0:
                scores = scores[np.argwhere(bools == True)]
                nat_idx = nat_idx[np.argwhere(bools == True)]
                nat_idx_max = nat_idx[np.argmax(scores)]
                df_gpt_np[i, columns["national_origin"]] = nat_idx_max
            else:
                bools = np.array([(nat[:-1] in u) or (u[:-1] in nat) for u in nat_unique.tolist()]).astype(bool)
                if bools.astype(int).sum() > 0:
                    scores = scores[np.argwhere(bools == True)]
                    nat_idx = nat_idx[np.argwhere(bools == True)]
                    nat_idx_max = nat_idx[np.argmax(scores)]
                    df_gpt_np[i, columns["national_origin"]] = nat_idx_max
                else:
                    df_gpt_np[i, columns["national_origin"]] = len(nat_unique)  # is it the right thing to use -1???

    for i in range(df_human_np.shape[0]):
        df_human_np[i, columns["file"]] = int(df_human_np[i, columns["file"]].split(".")[0])
        # if df_human_np[i, columns["file"]] not in picked_files:
        #     idx_human.append(i)
        if "id_fied" in columns.keys():
            if "speaker" not in df_human_np[i, columns["id_fied"]].lower():
                # print("exception spk human: speaker not in value", df_human_np[i, columns["id_fied"]].lower())
                df_human_np[i, columns["id_fied"]] = "-1"
                if "id_fier" not in columns.keys():
                    idx_human.append(i)
                    continue
            df_human_np[i, columns["id_fied"]] = int(df_human_np[i, columns["id_fied"]].lower().replace("speaker_", ""))

        if "id_fier" in columns.keys():
            df_human_np[i, columns["id_fier"]] = int(df_human_np[i, columns["id_fier"]].lower().replace("speaker_", ""))

        if "type" in columns.keys():
            df_human_np[i, columns["type"]] = valid_types.index(df_human_np[i, columns["type"]])

        if "race" in columns.keys():
            try:
                df_human_np[i, columns["race"]] = valid_races.index(df_human_np[i, columns["race"]])
            except Exception as e:
                # print(e)
                idx_human.append(i)

    df_gpt_np = np.delete(df_gpt_np, (idx_gpt), axis=0)
    df_human_np = np.delete(df_human_np, (idx_human), axis=0)

    df_human_np = np.unique(df_human_np.astype(int), axis=0)
    df_gpt_np = np.unique(df_gpt_np.astype(int), axis=0)
    # print("shape after", df_gpt_np.shape, df_human_np.shape)
    np.set_printoptions(threshold=sys.maxsize)

    nrows, ncols = max(df_gpt_np.shape[0], df_human_np.shape[0]), df_gpt_np.shape[1]
    dtype = {'names': ['f{}'.format(i) for i in range(ncols)],
             'formats': ncols * [df_gpt_np.dtype]}

    common_rows = np.intersect1d(df_gpt_np.view(dtype), df_human_np.view(dtype))

    common_rows = common_rows.view(df_gpt_np.dtype).reshape(-1, ncols)
    tp = common_rows.shape[0]
    fp = df_gpt_np.shape[0] - tp
    fn = df_human_np.shape[0] - tp

    print("Precision: ", tp / (tp + fp), "Recall: ", tp / (tp + fn), "F1: ", tp / (tp + ((fp + fn) / 2)))


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-c',
                        '--file_path_gpt',
                        help="path to csv annotation from chat gpt",
                        type=str,
                        required=True)
    parser.add_argument('-f',
                        '--file_path_human',
                        help="path to csv annotation from human",
                        type=str,
                        required=True)
    parser.add_argument('-m',
                        '--columns',
                        help="which attributes to evaluate. valid names: id_fier, id_fied, type, ethnicity, national_origin, race",
                        type=str,
                        nargs='+',
                        required=True)
    parser.add_argument("--preprocess", default=False, action="store_true")


    args = parser.parse_args()
    main(**vars(args))
