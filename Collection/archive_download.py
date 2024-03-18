#!/usr/bin/env python
import gzip
import json
import sys
import os
import shutil
from jsonargparse import (ArgumentParser, namespace_to_dict)
from xml.dom import minidom
import internetarchive as ia
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor


def prepare_list_of_ids(all_ids_file, done_ids_file=None):
    f = open(all_ids_file, 'r')
    all_ids = f.readlines()
    all_ids = [id.replace("\n", "") for id in all_ids]
#    all_ids = [(id.split(" "))[0] for id in all_ids if not (id=="" or id is None)]
    all_ids = [id.strip() for id in all_ids if not (id=="" or id is None)]
    f.close()

    done_ids = []
    if done_ids_file is not None:
        f = open(done_ids_file, 'r')
        done_ids = f.readlines()
        done_ids = [id.replace("\n","") for id in done_ids]
        done_ids = [id for id in done_ids if not (id=="" or id is None)]
        f.close()

    remaining_ids = []
    for id in all_ids:
        if id not in done_ids:
            remaining_ids.append(id)

    return remaining_ids


def remove_dir(save_directory, identifier):
    print("---------------------------->", identifier, " failed <----------------------------\n")
    try:
        shutil.rmtree(os.path.join(save_directory, identifier))
    except:
        os.remove(os.path.join(save_directory, identifier))


def download_captions(identifier, save_directory, file_elements):
    # Find out if a caption file is downloadable
    caption = ['cc5.txt', 'cc5.srt', 'asr.js', 'asr.srt', 'align.srt', 'align.json']
    files = []
    for c in caption:
        files = files + [f.attributes['name'].value for f in file_elements \
                         if f.attributes['name'].value.lower().endswith(c)]

    # Download caption files
    for file_ in files:
        try:
            ia.download(identifier, destdir=save_directory, glob_pattern=file_, ignore_existing=True,
                        no_change_timestamp=True, ignore_errors=True)
            return True
        except:
            print(identifier, " failed caption download ", file_)

    return False


def find_original_file(identifier, save_directory, file_elements, media):
    print("---------------------------->No mp3/mp4 ", identifier, "<----------------------------------\n")
    all_vid_extensions = ['.m4v', '.3gp', '.wmv', '.mkv', '.avi', '.flv', '.gif', '.3g2', '.webm', '.gifv', '.mpg', '.mp2', '.m2v', '.mpeg', '.mpe', '.mpv', '.amv', '.flv', '.f4v', '.f4p', '.f4a', '.f4b', '.vob', '.mts', '.m2ts', '.ts', '.mov', '.qt', '.yuv', '.rm', '.rmvb', '.svi']
    all_aud_extensions = ['.m4a', '.m4b', '.m4p', '.mp3', '.wav', '.rf64', '.webm', '.wv', '.raw', '.ra', '.rm', '.wma', '.3gp', '.aac', '.au', '.aiff', '.gsm', '.amr', '.awb', '.msv', '.nmf', '.aa', '.flac', '.aax', '.act', '.aiff', '.alac', '.mmf', '.opus', '.dfv', '.ape', '.dss', '.ogg', '.oga', '.mogg', '.8svx', '.voc', '.vox', '.sln', '.tta', '.cda', '.iklax', '.ivs']

    files = [f.attributes['name'].value for f in file_elements
             if ( # ((f.attributes['source']).value == "original") and
                 (not (f.attributes['name']).value.lower().endswith('.xml')) and
                 (not (f.attributes['name']).value.lower().endswith('.sqlite')) and
                 (not (f.attributes['name']).value.lower().endswith('.zip')) and
                 (not (f.attributes['name']).value.lower().endswith('.torrent')) and
                 (not (f.attributes['name']).value.lower().endswith('.txt')) and
                 (not (f.attributes['name']).value.lower().endswith('.jpg')))]
    if len(files) == 0:
        remove_dir(save_directory, identifier)
        return None

    files_all = files
    files = []

    if media in ['both', 'movies']:
        for ex in all_vid_extensions:
            files = files + [f for f in files_all if f.lower().endswith(ex)]
    if media in ['both', 'audio']:
        for ex in all_aud_extensions:
            files = files + [f for f in files_all if f.lower().endswith(ex)]

    if len(files) == 0:
        print('no content')
        remove_dir(save_directory, identifier)
        return None

    return files


def is_cc_licensed(save_directory, identifier):
    xml_file = [file_item for file_item in os.listdir(os.path.join(save_directory, identifier))
                if file_item.endswith('_meta.xml')]
    if len(xml_file) == 0:
        return False
    xml_file = minidom.parse(os.path.join(save_directory, identifier, xml_file[0]))
    license = xml_file.getElementsByTagName('licenseurl')

    if len(license):
        license = license[0]
        license = license.firstChild.data

        if 'creativecommons' in str(license).lower():
            return True
    return False


def download_data(id_list_file, save_directory, media='both', check_cc=True, get_caption=True, done_ids_file=None):
    def get_data(identifier):
        if os.path.exists(os.path.join(save_directory, identifier)):
            remove_dir(save_directory, identifier)
            return

        # download metadata (_files.xml, _meta.xml)
        try:
            ia.download(identifier, destdir=save_directory, glob_pattern="*_files.xml", ignore_existing=True, no_change_timestamp=True, ignore_errors=True)
            ia.download(identifier, destdir=save_directory, glob_pattern="*_meta.xml", ignore_existing=True, no_change_timestamp=True, ignore_errors=True)
        except Exception as e:
            print("missing xml")
            remove_dir(save_directory, identifier)
            return

        # Read meta data
        try:
            # check if CC licensed
            if check_cc and (not is_cc_licensed(save_directory, identifier)):
                print("not cc")
                remove_dir(save_directory, identifier)
                return

            xml_file = [f for f in os.listdir(os.path.join(save_directory, identifier)) if f.endswith('_files.xml')]
            if len(xml_file) == 0:
                print("missing files xml")
                remove_dir(save_directory, identifier)
                return

            xml_file = minidom.parse(os.path.join(save_directory, identifier, xml_file[0]))
            xml_file = xml_file.getElementsByTagName("file")

            # download captions
            if get_caption:
                download_captions(identifier, save_directory, xml_file)

            # Find out if an mp3 or mp4 file is downloadable
            files = [f.attributes['name'].value for f in xml_file \
                     if ((media in ['both', 'audio']) * f.attributes['name'].value.lower().endswith('mp3')) \
                     or ((media in ['both', 'movies']) * f.attributes['name'].value.lower().endswith('mp4'))]

            # If no mp3 or mp4 file is downloadable then find out the original file extension
            if len(files) == 0:
                files = find_original_file(identifier, save_directory, xml_file, media)
                if files is None:
                    return

            # Download file with chosen extension
            for file_pat in files:
                ia.download(identifier, destdir=save_directory, glob_pattern=file_pat, ignore_existing=True, no_change_timestamp=True, ignore_errors=True)

        except Exception as e:
            print('exceptipn', e)
            remove_dir(save_directory, identifier)

    if (id_list_file is None) or (save_directory is None) or (not os.path.exists(id_list_file)):
        return

    if isinstance(id_list_file, str) and id_list_file.endswith('.txt'):
        ids = prepare_list_of_ids(id_list_file, done_ids_file)
        ids = [m for m in ids if not (m == "" or m is None)]
    else:
        ids = []
        with gzip.open(id_list_file, "rt") as fh:
            for line in fh:
                ids.append(json.loads(line)["identifier"])
        ids = [m for m in ids if not (m == "" or m is None)]

    with ThreadPoolExecutor(5) as executor:
        list(tqdm(executor.map(get_data, ids), total=len(ids)))


if __name__ == '__main__':
    parser = ArgumentParser(description="Download files from archive")
    parser.add_argument('-f', '--id_list_file', help="path to file containing list of ids ", type=str, required=True)
    parser.add_argument('-d', '--save_directory', help='path of directory to save files', type=str, required=True)
    parser.add_argument('-m', '--media', default='both', help='Media type. options: audio, movies, both', type=str, required=False)
    parser.add_argument('-l', '--check_cc', default=True, help='To download CC-licensed content only', type=bool, required=False)
    parser.add_argument('-c', '--get_caption', default=True, help='To download captions if available', type=bool, required=False)
    parser.add_argument('-a', '--done_ids_file', default=None, help='path to file containing list of ids that are already downloaded', type=str, required=False)

    args = parser.parse_args()
    download_data(**namespace_to_dict(args))
