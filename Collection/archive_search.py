#!/usr/bin/env python

from tqdm import tqdm
import internetarchive as ia
from jsonargparse import (ArgumentParser, namespace_to_dict)
from concurrent.futures import ThreadPoolExecutor


def get_query():
    content = ['podcast', 'conversation', 'discussion', 'debate', 'interview', 'talk',
               'talks', 'talk show', 'guest talk', 'dialog', 'dialogue', 'tedtalk']
    keywords = ['latin american', 'latinx american', 'latino american', 'latina american',
                'mexican american', 'cuban american', 'costa rican american',
                'puerto rican american', 'brazilian american', 'colombian american']
    keywords += ['asian american', 'indian american', 'korean american', 'chinese american',
               'japanese american', 'south asian american', 'middle eastern american',
               'vietnamese american', 'indonesian american', 'malaysian americal']
    keywords += ['african american', 'black american', 'nigerian american', 'sudanese american', 
                 'egyptian american', 'libyan american', 'algerian american']

    query = '(mediatype:movies OR mediatype:audio) AND ('
    for i, c in enumerate(content):
        if i:
            query += ' OR '
        query += '(title:(' + c + ') OR description:(' + c + ') OR creator:(' + c + ') OR subject:(' + c + '))'
    query += ') AND ('
    for i, keyword in enumerate(keywords):
        if i:
            query += ' OR '
        query += '(title:(' + keyword + ') OR description:(' + keyword + ') OR creator:(' + keyword + ') OR subject:(' + c + '))'
    query += ') AND (NOT access-restricted-item:TRUE)'

    return query


def archive_search(save_file, check_cc=True):
    query = get_query()
    search = ia.search_items(query)
    all_results = list(tqdm(search.iter_as_results()))
    print("Number of results found: ", len(all_results))

    def get_metadata(result):
        item = ia.get_item(result["identifier"], archive_session=search.session)
        metadata = (item.item_metadata)['metadata']
        return metadata

    with ThreadPoolExecutor(15) as executor:
        metadata_list = list(tqdm(executor.map(get_metadata, all_results), total=len(all_results)))

    try:
        file_r = open(save_file, 'r')
        unique_metadata = file_r.readlines()
        unique_metadata = [id.replace("\n", "") for id in unique_metadata]
        unique_metadata = [id for id in unique_metadata if not (id == "" or id is None)]
        file_r.close()
    except:
        print("new file")
        unique_metadata = []

    filew = open(save_file, "a")
    for metadata in metadata_list:
        if check_cc and ('licenseurl' in metadata.keys()) and ('creativecommons' in str(metadata['licenseurl']).lower()):
            if metadata["identifier"] not in unique_metadata:
                unique_metadata.append(metadata["identifier"])
                filew.write(metadata["identifier"] + " " + metadata['licenseurl'])
                filew.write("\n")
    filew.close()
    print("\ntotal len ", len(unique_metadata))


if __name__ == '__main__':
    parser = ArgumentParser(description="Download files from archive")
    parser.add_argument('-f', '--save_file', help="path to save file containing list of ids ", type=str, required=True)
    parser.add_argument('-l', '--check_cc', default=True, help='To download CC-licensed content only', type=bool, required=False)

    args = parser.parse_args()
    archive_search(**namespace_to_dict(args))
